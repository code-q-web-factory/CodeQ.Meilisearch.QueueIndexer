<?php

declare(strict_types=1);

namespace CodeQ\Meilisearch\QueueIndexer\Command;

use CodeQ\Meilisearch\QueueIndexer\IndexingJob;
use Flowpack\JobQueue\Common\Exception;
use Flowpack\JobQueue\Common\Job\JobManager;
use Flowpack\JobQueue\Common\Queue\QueueManager;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Neos\ContentRepository\Domain\Factory\NodeFactory;
use Neos\ContentRepository\Domain\Projection\Content\TraversableNodeInterface;
use Neos\ContentRepository\Domain\Service\ContentDimensionCombinator;
use Neos\ContentRepository\Domain\Service\ContextFactoryInterface;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Cli\CommandController;
use Neos\Flow\Cli\Exception\StopCommandException;
use Neos\Flow\Log\ThrowableStorageInterface;
use Neos\Flow\Log\Utility\LogEnvironment;
use Neos\Flow\Persistence\PersistenceManagerInterface;
use Psr\Log\LoggerInterface;

/**
 * CLI commands for the live Meilisearch indexing queue.
 *
 * - `nodeindexqueue:work`   drains jobs produced by
 *   {@see \CodeQ\Meilisearch\QueueIndexer\Indexer\NodeIndexer} and executed by
 *   {@see \CodeQ\Meilisearch\QueueIndexer\IndexingJob}. It is a daemon unless
 *   `--exit-when-empty` is given.
 * - `nodeindexqueue:build`  walks the live tree once and enqueues one job per
 *   fulltext-root document. Use this instead of the synchronous upstream
 *   `./flow nodeindex:build` on large sites: per-document fulltext extraction
 *   in the upstream command runs in a single long-lived process whose Doctrine
 *   identity map grows unboundedly. The worker resets its read snapshot before
 *   each job instead of retaining entities across document extractions.
 *
 * @Flow\Scope("singleton")
 */
class NodeIndexQueueCommandController extends CommandController
{
    public const LIVE_QUEUE_NAME = 'CodeQ.Meilisearch.QueueIndexer.Live';

    /**
     * @Flow\Inject
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * @Flow\Inject
     * @var ThrowableStorageInterface
     */
    protected $throwableStorage;

    /**
     * @Flow\Inject
     * @var JobManager
     */
    protected $jobManager;

    /**
     * @Flow\Inject
     * @var QueueManager
     */
    protected $queueManager;

    /**
     * @Flow\Inject
     * @var ContextFactoryInterface
     */
    protected $contextFactory;

    /**
     * @Flow\Inject
     * @var ContentDimensionCombinator
     */
    protected $contentDimensionCombinator;

    /**
     * @Flow\Inject
     * @var PersistenceManagerInterface
     */
    protected $persistenceManager;

    /**
     * @Flow\Inject
     * @var NodeFactory
     */
    protected $nodeFactory;

    /**
     * Work the live indexing queue
     *
     * By default this is a daemon: when the queue runs dry the worker keeps
     * waiting for new jobs, and only `--exit-after` or `--limit` ever stop it.
     * That is what production wants for live indexing, but it makes the worker
     * unusable for draining a finite batch, because `--limit` counts *executed*
     * jobs: a limit that is higher than the number of queued jobs is never
     * reached and the command blocks forever. Pass `--exit-when-empty` to drain
     * the queue once and return - that is the mode to use after
     * `nodeindexqueue:build`.
     *
     * @param int|null $exitAfter If set, stop after this many seconds
     * @param int|null $limit Stop after this many job executions. Failed executions count towards the limit, and on its own the limit never stops a worker whose queue holds fewer jobs - combine it with --exit-when-empty
     * @param bool $verbose Print per-job debugging information
     * @param bool $exitWhenEmpty Stop as soon as no job can be reserved any more instead of waiting for new ones. Use this to drain the queue
     * @return void
     * @throws StopCommandException
     */
    public function workCommand(
        ?int $exitAfter = null,
        ?int $limit = null,
        bool $verbose = false,
        bool $exitWhenEmpty = false
    ): void {
        $queueName = self::LIVE_QUEUE_NAME;

        if ($exitAfter !== null && $exitAfter <= 0) {
            $this->outputLine('<error>--exit-after must be a positive integer; got %d</error>', [$exitAfter]);
            $this->quit(1);
        }
        if ($limit !== null && $limit <= 0) {
            $this->outputLine('<error>--limit must be a positive integer; got %d</error>', [$limit]);
            $this->quit(1);
        }

        if ($verbose) {
            $this->outputLine('Watching queue <b>"%s"</b>%s%s', [
                $queueName,
                $exitAfter !== null ? sprintf(' for <b>%d</b> seconds', $exitAfter) : '',
                $exitWhenEmpty ? ' until it is empty' : '',
            ]);
        }

        $startTime = time();
        $numberOfJobExecutions = 0;
        // Counts consecutive waitAndExecute() failures so we can back off if the queue
        // backend is persistently broken (DB down, table gone). Resets on any non-throwing
        // iteration so a single bad job does not induce a long pause on the next try.
        $consecutiveFailures = 0;
        $maxBackoffSeconds = 10;

        do {
            $timeout = $exitAfter !== null
                ? max(1, $exitAfter - (time() - $startTime))
                : null;
            if ($exitWhenEmpty) {
                // Poll in short slices so an empty queue is noticed within about a second
                // instead of after the backend's own reserve timeout (60s for DoctrineQueue).
                $timeout = $timeout !== null ? min($timeout, 1) : 1;
            }
            $message = null;
            $executionFailed = false;

            try {
                $this->resetReadSnapshot();
                $message = $this->jobManager->waitAndExecute($queueName, $timeout);
                $consecutiveFailures = 0;
            } catch (\Throwable $exception) {
                // Catch Throwable (not just Exception) so PHP 8 Errors - TypeError, ValueError,
                // AssertionError raised inside a job during node rehydration - do not escape
                // and crash the worker loop. This keeps --limit / --exit-after honoured even
                // when a single job is malformed.
                // A failure also means the queue was not empty, so --exit-when-empty must not
                // read this iteration's null message as a drained queue.
                $executionFailed = true;
                $numberOfJobExecutions++;
                $consecutiveFailures++;
                $verbose && $this->outputLine('<error>%s</error>', [$exception->getMessage()]);

                $previous = $exception->getPrevious();
                if ($previous instanceof \Throwable) {
                    $verbose && $this->outputLine('  Reason: %s', [$previous->getMessage()]);
                }
                if ($consecutiveFailures === 1) {
                    // Only the first failure of a streak keeps its stack trace: a broken backend
                    // fails every retry and would otherwise store one dump or error event per attempt.
                    $details = $this->throwableStorage->logThrowable($exception);
                } elseif ($previous instanceof \Throwable) {
                    $details = sprintf('%s. Reason: %s', $exception->getMessage(), $previous->getMessage());
                } else {
                    $details = $exception->getMessage();
                }
                $this->logger->error('Meilisearch indexing job failed: ' . $details, LogEnvironment::fromMethodName(__METHOD__));

                // 1s, 2s, 4s, 8s, 10s, 10s... capped so we never disappear for a long time.
                $sleepSeconds = min(2 ** min($consecutiveFailures - 1, 3), $maxBackoffSeconds);
                $verbose && $this->outputLine('  Backing off %ds before retry', [$sleepSeconds]);
                sleep($sleepSeconds);
            }

            if ($message !== null) {
                $numberOfJobExecutions++;
                if ($verbose) {
                    $payload = strlen($message->getPayload()) <= 50
                        ? $message->getPayload()
                        : substr($message->getPayload(), 0, 50) . '...';
                    $this->outputLine('<success>Executed job "%s" (%s)</success>', [$message->getIdentifier(), $payload]);
                }
            }

            if ($exitAfter !== null && (time() - $startTime) >= $exitAfter) {
                $this->outputRemainingQueueDepth(
                    sprintf('Quitting after %d seconds due to --exit-after', time() - $startTime)
                );
                $this->quit();
            }

            if ($limit !== null && $numberOfJobExecutions >= $limit) {
                $this->outputRemainingQueueDepth(sprintf(
                    'Quitting after %d job execution%s due to --limit',
                    $numberOfJobExecutions,
                    $numberOfJobExecutions === 1 ? '' : 's'
                ));
                $this->quit();
            }

            if ($exitWhenEmpty && $message === null && $executionFailed === false) {
                $this->outputRemainingQueueDepth('Quitting due to --exit-when-empty: no job left to reserve');
                $this->quit();
            }
        } while (true);
    }

    /**
     * Print why the worker stops together with what is still in the queue.
     *
     * Every exit path needs this: `--limit` and `--exit-after` stop on a budget,
     * not on an empty queue, and even `--exit-when-empty` only proves that
     * nothing was reservable in the last poll - another worker can hold reserved
     * jobs, and a job released with a delay stays "ready" until it is due. Saying
     * so here is what keeps an aborted drain from looking finished.
     */
    protected function outputRemainingQueueDepth(string $reason): void
    {
        try {
            $queue = $this->queueManager->getQueue(self::LIVE_QUEUE_NAME);
            $this->outputLine('%s. Queue left at ready: %d, reserved: %d, failed: %d', [
                $reason,
                $queue->countReady(),
                $queue->countReserved(),
                $queue->countFailed(),
            ]);
        } catch (\Throwable $exception) {
            $this->outputLine('%s. Queue depth unavailable: %s', [$reason, $exception->getMessage()]);
        }
    }

    protected function resetReadSnapshot(): void
    {
        // Jobs only read the CR. A long-lived worker must not reuse entities or
        // context/node caches hydrated before another request published a change.
        $this->contextFactory->reset();
        $this->nodeFactory->reset();
        $this->persistenceManager->clearState();
    }

    /**
     * Enqueue one IndexingJob per fulltext-root document and allowed dimension
     * combination in the live workspace.
     *
     * Use this as a memory-safe alternative to `./flow nodeindex:build`. Once it
     * returns, start a worker to actually process the jobs:
     *
     *     ./flow nodeindexqueue:work --exit-when-empty --verbose
     *
     * `--exit-when-empty` is what makes that worker return. Without it the worker
     * is a daemon that waits for more jobs forever, and `--limit` does not help:
     * it stops after that many *executions*, so a limit above the number of queued
     * jobs is never reached. Check the result with `nodeindexqueue:status`; the
     * build is applied once ready, reserved and failed are all zero.
     *
     * The walk only collects node references (not content subtrees), which
     * keeps peak memory well below the synchronous build. Each document is
     * then fulltext-extracted and written with a fresh worker read snapshot, so
     * Doctrine's identity map cannot balloon the way it does in the upstream
     * single-process walk.
     *
     * @param bool $verbose Print per-batch progress
     * @return void
     */
    public function buildCommand(bool $verbose = false): void
    {
        $dimensionCombinations = $this->contentDimensionCombinator->getAllAllowedCombinations();
        if ($dimensionCombinations === []) {
            $dimensionCombinations = [[]];
        }

        $this->outputLine('Enqueueing indexing jobs onto <b>%s</b>...', [self::LIVE_QUEUE_NAME]);
        $enqueued = 0;

        foreach ($dimensionCombinations as $targetDimensionCombination) {
            $contextProperties = ['workspaceName' => 'live'];
            if ($targetDimensionCombination !== []) {
                $contextProperties['dimensions'] = $targetDimensionCombination;
            }

            $this->outputLine('  Dimension: %s', [self::formatDimensionCombination($targetDimensionCombination)]);
            $context = $this->contextFactory->create($contextProperties);
            $rootNode = $context->getRootNode();

            $enqueued = $this->enqueueTreeRecursively(
                $rootNode,
                $enqueued,
                $verbose,
                $targetDimensionCombination
            );
            $this->persistenceManager->clearState();
        }

        $this->outputLine('<success>Enqueued %d job%s.</success>', [$enqueued, $enqueued === 1 ? '' : 's']);
        $this->outputLine('Drain with: ./flow nodeindexqueue:work --exit-when-empty --verbose');
    }

    /**
     * Depth-first walk that enqueues an IndexingJob at every fulltext-root.
     * Returns the cumulative enqueue count so the caller can report a total.
     */
    protected function enqueueTreeRecursively(
        NodeInterface $node,
        int $counter,
        bool $verbose,
        array $targetDimensionCombination = []
    ): int {
        if (self::isFulltextRoot($node)) {
            $payload = [
                'persistenceObjectIdentifier' => $this->persistenceManager->getIdentifierByObject($node->getNodeData()),
                'identifier' => $node->getIdentifier(),
                'dimensions' => $targetDimensionCombination !== []
                    ? $targetDimensionCombination
                    : $node->getContext()->getDimensions(),
                'workspace' => $node->getWorkspace()->getName(),
                'nodeType' => $node->getNodeType()->getName(),
                'path' => $node->getPath(),
            ];
            try {
                $this->jobManager->queue(
                    self::LIVE_QUEUE_NAME,
                    new IndexingJob(null, $payload, false, false, $targetDimensionCombination)
                );
            } catch (\Throwable $exception) {
                $this->logger->error(
                    sprintf('Failed to enqueue indexing job for %s: %s', $node->getPath(), $exception->getMessage()),
                    LogEnvironment::fromMethodName(__METHOD__)
                );
                $this->outputLine('<error>Failed to enqueue %s: %s</error>', [$node->getPath(), $exception->getMessage()]);
                return $counter;
            }
            $counter++;

            if ($verbose && $counter % 50 === 0) {
                $this->outputLine('  %d enqueued (mem: %s)', [$counter, self::formatBytes(memory_get_usage(true))]);
            }
        }

        if (!$node instanceof TraversableNodeInterface) {
            throw new \LogicException('Snapshot traversal requires a traversable content repository node.');
        }
        foreach ($node->findChildNodes() as $childNode) {
            $counter = $this->enqueueTreeRecursively(
                $childNode,
                $counter,
                $verbose,
                $targetDimensionCombination
            );
        }

        return $counter;
    }

    /**
     * Mirrors Medienreaktor.Meilisearch's isFulltextRoot check so we enqueue
     * exactly the nodes the upstream sync build would have indexed.
     */
    protected static function isFulltextRoot(NodeInterface $node): bool
    {
        $search = $node->getNodeType()->getConfiguration('search');
        return is_array($search)
            && isset($search['fulltext']['isRoot'])
            && $search['fulltext']['isRoot'] === true;
    }

    protected static function formatBytes(int $bytes): string
    {
        $units = ['B', 'KB', 'MB', 'GB'];
        $i = 0;
        while ($bytes >= 1024 && $i < count($units) - 1) {
            $bytes /= 1024;
            $i++;
        }
        return sprintf('%.1f %s', $bytes, $units[$i]);
    }

    protected static function formatDimensionCombination(array $dimensionCombination): string
    {
        if ($dimensionCombination === []) {
            return '(none)';
        }

        $labels = [];
        foreach ($dimensionCombination as $dimensionName => $dimensionValues) {
            $labels[] = sprintf('%s: %s', $dimensionName, implode(',', $dimensionValues));
        }

        return implode('; ', $labels);
    }

    /**
     * Print queue depth counters
     *
     * The counters are the three states the JobQueue backend actually stores, and
     * they are labelled with those state names so the output can be compared with
     * the backend without guessing. The resolved backend class and - for a Doctrine
     * queue - the table name are printed too, because the queue table is a
     * configuration detail: cross-checking a count against a leftover table from an
     * earlier indexer is what makes a correct count look wrong.
     *
     *     select state, count(*) from <table> group by state
     *
     * "ready" does not mean "never tried". The JobManager releases a failing job
     * back to "ready" and increments its failure counter until
     * maximumNumberOfReleases is exhausted; only then does it become "failed". A
     * rebuild is therefore finished when all three counters are zero, not when
     * "failed" looks small.
     */
    public function statusCommand(): void
    {
        $queueName = self::LIVE_QUEUE_NAME;
        $this->outputLine('<b>%s</b>', [$queueName]);

        try {
            $queue = $this->queueManager->getQueue($queueName);
            $queueSettings = $this->queueManager->getQueueSettings($queueName);
            $ready = $queue->countReady();
            $reserved = $queue->countReserved();
            $failed = $queue->countFailed();
        } catch (\Throwable $exception) {
            // Not just Flowpack's Exception: an unconfigured queue throws that one, but a
            // missing table throws \RuntimeException and a dead connection a DBAL exception.
            // Those used to escape and bury the cause under a Flow stack trace.
            $this->outputLine('  <error>Queue not available: %s</error>', [$exception->getMessage()]);
            return;
        }

        $maximumNumberOfAttempts = 1 + (int)($queueSettings['maximumNumberOfReleases']
            ?? JobManager::DEFAULT_MAXIMUM_NUMBER_RELEASES);
        $tableName = $queueSettings['options']['tableName'] ?? null;

        $this->outputLine('  Backend          : %s', [$queueSettings['className'] ?? get_class($queue)]);
        if (is_string($tableName) && $tableName !== '') {
            $this->outputLine('  Table            : %s', [$tableName]);
        }
        $this->outputLine('  state "ready"    : %d  waiting for a worker, including jobs released for a retry', [$ready]);
        $this->outputLine('  state "reserved" : %d  currently held by a worker', [$reserved]);
        $this->outputLine('  state "failed"   : %d  given up after %d attempt%s', [
            $failed,
            $maximumNumberOfAttempts,
            $maximumNumberOfAttempts === 1 ? '' : 's',
        ]);
        $this->outputLine('  rows in total    : %d', [$ready + $reserved + $failed]);

        if ($ready + $reserved + $failed === 0) {
            $this->outputLine('  <success>Queue is empty.</success>');
        }
    }

    /**
     * Drain the live queue
     */
    public function flushCommand(): void
    {
        try {
            $this->queueManager->getQueue(self::LIVE_QUEUE_NAME)->flush();
            $this->outputLine('<success>Flushed queue %s</success>', [self::LIVE_QUEUE_NAME]);
        } catch (Exception $exception) {
            $this->outputLine('<error>Flush failed: %s</error>', [$exception->getMessage()]);
        }
    }
}
