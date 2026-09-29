import { JobState } from '@vendure/common/lib/generated-types';
import { ID } from '@vendure/common/lib/shared-types';
import { randomUUID } from 'crypto';
import type { Client, ClientConfig, Notification } from 'pg';
import { DataSource } from 'typeorm';
import { PostgresConnectionOptions } from 'typeorm/driver/postgres/PostgresConnectionOptions';

import { Injector } from '../../common/injector';
import { Logger } from '../../config/logger/vendure-logger';
import { getDatabaseType } from '../../connection/database-type';
import { TransactionalConnection } from '../../connection/transactional-connection';
import { Job, JobData, JobQueueStrategyJobOptions } from '../../job-queue';
import { PollingJobQueueStrategyConfig } from '../../job-queue/polling-job-queue-strategy';

import { JobRecord } from './job-record.entity';
import { SqlJobQueueStrategy } from './sql-job-queue-strategy';

const CHANNEL = 'vendure_job';
const loggerCtx = 'PgNotifyJobQueueStrategy';

const DEFAULT_SAFETY_INTERVAL_MS = 5 * 60 * 1000;
const MAX_RECONNECT_DELAY_MS = 30_000;
const INITIAL_RECONNECT_DELAY_MS = 1_000;
const PROBE_TIMEOUT_MS = 3_000;
/** Not a valid queue name in practice, so `wake()` finds no waiters for it. */
const PROBE_PREFIX = '__vendure_job_probe__:';
/**
 * How long a retry is still tracked after its backoff has elapsed. `SqlJobQueueStrategy`
 * compares the backoff against the row's `updatedAt`, which is set by the database clock,
 * so a retry can become due slightly later than this process expects.
 */
const RETRY_GRACE_MS = 5_000;
const MIN_RETRY_WAIT_MS = 50;

/**
 * @description
 * Configuration options for the {@link PgNotifyJobQueueStrategy}.
 *
 * @docsCategory JobQueue
 * @since 3.8.0
 */
export interface PgNotifyJobQueueStrategyConfig extends PollingJobQueueStrategyConfig {
    /**
     * @description
     * Connection details for the dedicated listener connection.
     *
     * The listener sits in `LISTEN` indefinitely, so it deliberately does not come from
     * TypeORM's pool - a pooled connection gets recycled out from under the subscription.
     * Left unset, the connection is built from the DataSource's own options, so a project
     * which already works needs no further configuration.
     *
     * Set this explicitly if the primary connection goes through a connection pooler:
     * poolers in transaction mode multiplex connections and silently drop `LISTEN`, so the
     * listener should be pointed at the direct database host. If the listener does not
     * receive a test notification on connect, the strategy logs a warning and polls.
     *
     * @default undefined
     */
    listenerConnection?: ClientConfig;
    /**
     * @description
     * How long a blocked call to `next()` waits for a notification before checking the
     * database anyway.
     *
     * This is a safety net, not a poll. `NOTIFY` is fire-and-forget: Postgres does not
     * queue notifications for a listener which is not connected, so a wake-up sent while
     * this process was reconnecting is simply lost. Without this timeout the corresponding
     * job would stay `PENDING` indefinitely. With it, the worst case for a lost
     * notification is one interval of lateness.
     *
     * @default 300_000
     */
    safetyIntervalMs?: number;
}

/**
 * @description
 * A {@link JobQueueStrategy} which is woken by Postgres `LISTEN`/`NOTIFY` rather than
 * polling the database for work.
 *
 * {@link SqlJobQueueStrategy} finds jobs by asking: each queue runs a
 * `BEGIN` / `SELECT ... FOR UPDATE` / `COMMIT` every `pollInterval` ms, with no backoff
 * when the answer keeps being "nothing". With the database on the same machine that is
 * free and unremarkable. With it on a managed host reached over the network - and
 * especially one which meters bandwidth - four idle queues cost on the order of 1.6
 * million queries a day whether or not a single request is served.
 *
 * This strategy replaces the asking with being told, and inherits everything else from
 * `SqlJobQueueStrategy` - `update`, `findMany`, the `FOR UPDATE` row locking and the
 * `InspectableJobQueueStrategy` surface which the Admin UI's job list reads.
 *
 * Measured on an otherwise idle project with four queues, against a local Postgres:
 *
 * | | `SqlJobQueueStrategy` | this strategy |
 * | --- | --- | --- |
 * | `job_record` queries / 30s | 564 | 0 |
 * | projected per day | 1,623,887 | ~2,304 |
 * | job pickup latency | 180ms | 76-79ms |
 *
 * Jobs also start *faster*, because a notification arrives when the row is committed
 * rather than at the next tick of a timer.
 *
 * @example
 * ```ts
 * import { DefaultJobQueuePlugin, VendureConfig } from '(at)vendure/core';
 *
 * export const config: VendureConfig = {
 *   // ...
 *   plugins: [
 *     DefaultJobQueuePlugin.init({ useNotify: true }),
 *   ],
 * };
 * ```
 *
 * Requires Postgres. On any other database the strategy logs a warning during
 * bootstrap and behaves exactly like {@link SqlJobQueueStrategy}, polling at
 * `pollInterval`.
 *
 * @docsCategory JobQueue
 * @since 3.8.0
 */
export class PgNotifyJobQueueStrategy extends SqlJobQueueStrategy {
    private listener?: Client;
    private txConnection?: TransactionalConnection;
    private dataSource?: DataSource;
    /** Queue name -> the `next()` calls currently parked on it. */
    private readonly waiters = new Map<string, Set<() => void>>();
    /**
     * Queues which have been stopped and not restarted. `stop()` can be called while a
     * `next()` is between asking the database and parking, in which case waking it finds
     * no waiter to release; this makes that `next()` decline to park at all.
     */
    private readonly stopping = new Set<string>();
    private readonly listenerConnection?: ClientConfig;
    private readonly safetyIntervalMs: number;
    /**
     * Whether the database can deliver notifications at all. When false, `next()` never
     * parks, which is what makes a misconfigured project slower rather than broken.
     */
    private notifySupported = false;
    /**
     * Queues which were woken while nothing was parked on them. The next `next()` to park
     * on such a queue re-checks the table immediately instead, so a notification which
     * arrives between asking the database and parking is not lost.
     */
    private readonly pendingWakes = new Set<string>();
    /**
     * Jobs this process has set to `RETRYING`, and when their backoff elapses. Nothing
     * notifies when a backoff elapses, so a queue with a pending retry parks only until
     * the retry is due.
     */
    private readonly retries = new Map<ID, { queueName: string; dueAt: number }>();
    private listenerStarted = false;
    private shuttingDown = false;
    private reconnectDelay = INITIAL_RECONNECT_DELAY_MS;

    constructor(config: PgNotifyJobQueueStrategyConfig = {}) {
        super(config);
        this.listenerConnection = config.listenerConnection;
        this.safetyIntervalMs = config.safetyIntervalMs ?? DEFAULT_SAFETY_INTERVAL_MS;
    }

    init(injector: Injector) {
        super.init(injector);
        this.txConnection = injector.get(TransactionalConnection);
        this.dataSource = this.txConnection.rawConnection;
        this.notifySupported = getDatabaseType(this.dataSource) === 'postgres';
        if (!this.notifySupported) {
            Logger.warn(
                'PgNotifyJobQueueStrategy requires Postgres, so the job queue will poll instead. ' +
                    'Use SqlJobQueueStrategy directly to silence this warning.',
                loggerCtx,
            );
        }
        // The listener is opened lazily, by the first queue which actually parks - see
        // `ensureListener()`. The config is loaded by the server process as well as the
        // worker, and only the worker calls `next()`, so connecting here would leave the
        // server holding a Postgres connection it never reads from.
    }

    /**
     * Asks the database once; if it has nothing, waits to be told rather than asking
     * again. Returning immediately when a job is already waiting is what lets a backlog
     * drain at full speed - only an empty queue ever parks.
     */
    async next(queueName: string): Promise<Job | undefined> {
        const job = await super.next(queueName);
        if (job || !this.notifySupported) {
            return job;
        }
        if (!this.listener) {
            // Until the listener is connected, and whenever it is down, a notification
            // would go unheard, so the queue polls at `pollInterval` instead of parking.
            this.ensureListener();
            return undefined;
        }
        await this.waitForWork(queueName);
        if (this.shuttingDown || this.stopping.has(queueName)) {
            // `ActiveQueue.stop()` has already stopped tracking jobs, so a job claimed now
            // would be left `RUNNING`.
            return undefined;
        }
        return super.next(queueName);
    }

    async update(job: Job<any>): Promise<void> {
        await super.update(job);
        if (job.id == null) {
            return;
        }
        if (job.state === JobState.RETRYING && this.backOffStrategy) {
            const delay = this.backOffStrategy(job.queueName, job.attempts, job);
            this.retries.set(job.id, { queueName: job.queueName, dueAt: Date.now() + delay });
        } else {
            this.retries.delete(job.id);
        }
    }

    /**
     * Enqueues the job, then wakes whoever is waiting on that queue.
     *
     * The `pg_notify` deliberately rides the *same* manager as the insert. `add()` uses
     * the request's transactional repository when it is given a `ctx`, and Postgres holds
     * notifications until `COMMIT` - so the worker is woken exactly when the row becomes
     * visible to it, and not at all if the transaction rolls back. Notifying over a
     * separate connection would race the commit: the worker would wake, query, find an
     * empty table, and sleep until the safety interval expired.
     */
    async add<Data extends JobData<Data> = object>(
        job: Job<Data>,
        jobOptions?: JobQueueStrategyJobOptions<Data>,
    ): Promise<Job<Data>> {
        const result = await super.add(job, jobOptions);
        if (!this.notifySupported) {
            return result;
        }
        const manager =
            jobOptions?.ctx && this.txConnection
                ? this.txConnection.getRepository(jobOptions.ctx, JobRecord).manager
                : this.dataSource?.manager;
        try {
            await manager?.query('SELECT pg_notify($1, $2)', [CHANNEL, job.queueName]);
        } catch (e: any) {
            // A wake-up which fails to send is late work, not lost work - the safety
            // interval still picks the job up. `pg_notify` itself only fails on an invalid
            // channel or a payload over 8000 bytes, neither of which a queue name produces,
            // so in practice this catches a lost connection. Postgres reports a full
            // notification queue at COMMIT instead, which this cannot catch.
            Logger.warn(`Could not notify queue "${job.queueName}": ${e.message as string}`, loggerCtx);
        }
        return result;
    }

    async start<Data extends JobData<Data> = object>(
        queueName: string,
        process: (job: Job<Data>) => Promise<any>,
    ): Promise<void> {
        this.stopping.delete(queueName);
        return super.start(queueName, process);
    }

    /**
     * Releases this queue's parked `next()`, so that a shutdown is not held up waiting for
     * a notification which is not coming.
     */
    async stop<Data extends JobData<Data> = object>(
        queueName: string,
        process: (job: Job<Data>) => Promise<any>,
    ): Promise<void> {
        this.stopping.add(queueName);
        this.wake(queueName);
        return super.stop(queueName, process);
    }

    destroy() {
        this.shuttingDown = true;
        this.wakeAll();
        const client = this.listener;
        this.listener = undefined;
        void client?.end().catch(() => undefined);
        super.destroy();
    }

    /**
     * Opens the listener on first use. Queues poll until it is connected.
     */
    private ensureListener() {
        if (this.listenerStarted || this.shuttingDown) {
            return;
        }
        this.listenerStarted = true;
        void this.connectListener();
    }

    private waitForWork(queueName: string): Promise<void> {
        if (this.shuttingDown || this.stopping.has(queueName)) {
            return Promise.resolve();
        }
        if (this.pendingWakes.delete(queueName)) {
            return Promise.resolve();
        }
        return new Promise<void>(resolve => {
            let settled = false;
            const done = () => {
                if (settled) {
                    return;
                }
                settled = true;
                clearTimeout(timer);
                this.waiters.get(queueName)?.delete(done);
                resolve();
            };
            const timer = setTimeout(done, this.parkTimeout(queueName));
            // A parked `next()` is a queue's normal resting state, so this timer is
            // pending almost always. Left referenced, it would hold the process open for
            // the full interval on every shutdown.
            timer.unref();
            let waiters = this.waiters.get(queueName);
            if (!waiters) {
                waiters = new Set();
                this.waiters.set(queueName, waiters);
            }
            waiters.add(done);
        });
    }

    /**
     * The safety interval, or less if a job this process set to `RETRYING` on this queue
     * becomes due sooner.
     */
    private parkTimeout(queueName: string): number {
        const now = Date.now();
        let timeout = this.safetyIntervalMs;
        for (const [id, retry] of this.retries) {
            if (now > retry.dueAt + RETRY_GRACE_MS) {
                // Picked up by another worker, or not due by the database's clock either.
                this.retries.delete(id);
            } else if (retry.queueName === queueName) {
                timeout = Math.min(timeout, Math.max(retry.dueAt - now, MIN_RETRY_WAIT_MS));
            }
        }
        return timeout;
    }

    private wake(queueName: string) {
        const waiters = this.waiters.get(queueName);
        if (!waiters?.size) {
            this.pendingWakes.add(queueName);
            return;
        }
        for (const done of [...waiters]) {
            done();
        }
    }

    private wakeAll() {
        for (const queueName of [...this.waiters.keys()]) {
            this.wake(queueName);
        }
    }

    /**
     * Builds the listener's connection details from the DataSource, so that a project
     * which already works needs no further configuration.
     */
    private clientConfig(): ClientConfig {
        if (this.listenerConnection) {
            return this.listenerConnection;
        }
        const options = this.dataSource?.options as PostgresConnectionOptions | undefined;
        // Mirrors how TypeORM's Postgres driver builds its pool config: credentials from
        // the replication master if there is one, then `extra` on top.
        const credentials = options?.replication?.master ?? options;
        return {
            connectionString: credentials?.url,
            host: credentials?.host,
            port: credentials?.port,
            user: credentials?.username,
            password: credentials?.password as string | undefined,
            database: credentials?.database,
            ssl: credentials?.ssl as ClientConfig['ssl'],
            application_name: options?.applicationName,
            ...options?.extra,
        };
    }

    private async connectListener(): Promise<void> {
        if (this.shuttingDown || !this.notifySupported) {
            return;
        }
        let client: Client;
        try {
            // Imported at call time rather than at the top of the file: `pg` is a peer of
            // TypeORM's Postgres driver rather than a dependency of Vendure, so a project
            // on MySQL or SQLite must not be made to resolve it.
            const { Client: PgClient } = await import('pg');
            client = new PgClient(this.clientConfig());
        } catch (e: any) {
            Logger.warn(
                `Could not load the "pg" package, so the job queue will poll instead: ${e.message as string}`,
                loggerCtx,
            );
            return;
        }
        client.on('notification', message => {
            if (message.payload) {
                this.wake(message.payload);
            }
        });
        const onLost = (e?: Error) => {
            if (this.listener !== client) {
                return;
            }
            this.listener = undefined;
            if (e) {
                Logger.warn(`Job queue listener lost: ${e.message}`, loggerCtx);
            }
            // Parked queues would not hear anything until the listener is back.
            this.wakeAll();
            this.scheduleReconnect();
        };
        client.on('error', onLost);
        client.on('end', () => onLost());

        try {
            await client.connect();
            await client.query(`LISTEN ${CHANNEL}`);
            if (!(await this.probe(client))) {
                // `this.listener` is not yet set, so ending the client does not trigger
                // `onLost()` and a reconnect.
                void client.end().catch(() => undefined);
                Logger.warn(
                    'Job queue listener did not receive a test notification, so the job queue will poll instead. ' +
                        'This usually means the connection goes through a pooler in transaction mode, ' +
                        'which drops LISTEN. Set `listenerConnection` to the direct database host.',
                    loggerCtx,
                );
                return;
            }
            // Notifications heard before any queue parked say nothing useful: the queues
            // were polling and saw those jobs anyway.
            this.pendingWakes.clear();
            this.listener = client;
            this.reconnectDelay = INITIAL_RECONNECT_DELAY_MS;
            Logger.verbose(`Job queue listening on "${CHANNEL}"`, loggerCtx);
        } catch (e: any) {
            void client.end().catch(() => undefined);
            Logger.warn(`Job queue listener could not connect: ${e.message as string}`, loggerCtx);
            this.scheduleReconnect();
        }
    }

    /**
     * Sends a notification through the regular pool and checks that the listener receives
     * it. A pooler in transaction mode accepts `LISTEN` without error but never delivers
     * anything, which would otherwise leave every job waiting for the safety interval.
     */
    private probe(client: Client): Promise<boolean> {
        const payload = PROBE_PREFIX + randomUUID();
        return new Promise(resolve => {
            const finish = (received: boolean) => {
                clearTimeout(timer);
                client.off('notification', onNotification);
                resolve(received);
            };
            const onNotification = (message: Notification) => {
                if (message.payload === payload) {
                    finish(true);
                }
            };
            const timer = setTimeout(() => finish(false), PROBE_TIMEOUT_MS);
            client.on('notification', onNotification);
            this.dataSource?.manager
                .query('SELECT pg_notify($1, $2)', [CHANNEL, payload])
                .catch(() => finish(false));
        });
    }

    private scheduleReconnect() {
        if (this.shuttingDown) {
            return;
        }
        const delay = this.reconnectDelay;
        this.reconnectDelay = Math.min(delay * 2, MAX_RECONNECT_DELAY_MS);
        const timer = setTimeout(() => void this.connectListener(), delay);
        timer.unref();
    }
}
