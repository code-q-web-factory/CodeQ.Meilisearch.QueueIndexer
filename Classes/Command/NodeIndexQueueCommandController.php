<?php

declare(strict_types=1);

namespace CodeQ\Meilisearch\QueueIndexer\Command;

use CodeQ\Meilisearch\QueueIndexer\IndexingJob;
use Flowpack\JobQueue\Common\Exception;
use Flowpack\JobQueue\Common\Job\JobManager;
use Flowpack\JobQueue\Common\Queue\QueueManager;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Neos\ContentRepository\Domain\Service\ContextFactoryInterface;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Cli\CommandController;
use Neos\Flow\Cli\Exception\StopCommandException;
use Neos\Flow\Log\Utility\LogEnvironment;
use Neos\Flow\Persistence\PersistenceManagerInterface;
use Psr\Log\LoggerInterface;

/**
 * CLI commands for the live Meilisearch indexing queue.
 *
 * - `nodeindexqueue:work`   drains jobs produced by
 *   {@see \CodeQ\Meilisearch\QueueIndexer\Indexer\NodeIndexer} and executed by
 *   {@see \CodeQ\Meilisearch\QueueIndexer\IndexingJob}.
 * - `nodeindexqueue:build`  walks the live tree once and enqueues one job per
 *   fulltext-root document. Use this instead of the synchronous upstream
 *   `./flow nodeindex:build` on large sites: per-document fulltext extraction
 *   in the upstream command runs in a single long-lived process whose Doctrine
 *   identity map grows unboundedly. The queue path isolates each document in
 *   its own worker invocation, keeping memory flat.
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
     * @var PersistenceManagerInterface
     */
    protected $persistenceManager;

    /**
     * Work the live indexing queue
     *
     * @param int|null $exitAfter If set, stop after this many seconds
     * @param int|null $limit Process at most this many jobs (successful or failed) before exiting
     * @param bool $verbose Print per-job debugging information
     * @return void
     * @throws StopCommandException
     */
    public function workCommand(?int $exitAfter = null, ?int $limit = null, bool $verbose = false): void
    {
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
            $this->outputLine('Watching queue <b>"%s"</b>%s', [
                $queueName,
                $exitAfter !== null ? sprintf(' for <b>%d</b> seconds', $exitAfter) : '',
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
            $message = null;

            try {
                $message = $this->jobManager->waitAndExecute($queueName, $timeout);
                $consecutiveFailures = 0;
            } catch (\Throwable $exception) {
                // Catch Throwable (not just Exception) so PHP 8 Errors - TypeError, ValueError,
                // AssertionError raised inside a job during node rehydration - do not escape
                // and crash the worker loop. This keeps --limit / --exit-after honoured even
                // when a single job is malformed.
                $numberOfJobExecutions++;
                $consecutiveFailures++;
                $verbose && $this->outputLine('<error>%s</error>', [$exception->getMessage()]);

                $previous = $exception->getPrevious();
                if ($previous instanceof \Throwable) {
                    $verbose && $this->outputLine('  Reason: %s', [$previous->getMessage()]);
                    $this->logger->error(
                        sprintf('Meilisearch indexing job failed: %s. Reason: %s', $exception->getMessage(), $previous->getMessage()),
                        LogEnvironment::fromMethodName(__METHOD__)
                    );
                } else {
                    $this->logger->error('Meilisearch indexing job failed: ' . $exception->getMessage(), LogEnvironment::fromMethodName(__METHOD__));
                }

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
                $verbose && $this->outputLine('Quitting after %d seconds due to --exit-after', [time() - $startTime]);
                $this->quit();
            }

            if ($limit !== null && $numberOfJobExecutions >= $limit) {
                $verbose && $this->outputLine('Quitting after %d job%s due to --limit', [$numberOfJobExecutions, $numberOfJobExecutions > 1 ? 's' : '']);
                $this->quit();
            }
        } while (true);
    }

    /**
     * Enqueue one IndexingJob per fulltext-root document in the live workspace.
     *
     * Use this as a memory-safe alternative to `./flow nodeindex:build`. Once it
     * returns, start a worker to actually process the jobs:
     *
     *     ./flow nodeindexqueue:work --verbose
     *
     * The walk only collects node references (not content subtrees), which
     * keeps peak memory well below the synchronous build. Each document is
     * then fulltext-extracted and written by an isolated job invocation, so
     * Doctrine's identity map cannot balloon the way it does in the upstream
     * single-process walk.
     *
     * @param bool $verbose Print per-batch progress
     * @return void
     */
    public function buildCommand(bool $verbose = false): void
    {
        $context = $this->contextFactory->create(['workspaceName' => 'live']);
        $rootNode = $context->getRootNode();
        if ($rootNode === null) {
            $this->outputLine('<error>Live workspace has no root node.</error>');
            $this->quit(1);
        }

        $this->outputLine('Enqueueing indexing jobs onto <b>%s</b>...', [self::LIVE_QUEUE_NAME]);
        $enqueued = $this->enqueueTreeRecursively($rootNode, 0, $verbose);
        $this->outputLine('<success>Enqueued %d job%s.</success>', [$enqueued, $enqueued === 1 ? '' : 's']);
        $this->outputLine('Drain with: ./flow nodeindexqueue:work --verbose');
    }

    /**
     * Depth-first walk that enqueues an IndexingJob at every fulltext-root.
     * Returns the cumulative enqueue count so the caller can report a total.
     */
    protected function enqueueTreeRecursively(NodeInterface $node, int $counter, bool $verbose): int
    {
        if (self::isFulltextRoot($node)) {
            $payload = [
                'persistenceObjectIdentifier' => $this->persistenceManager->getIdentifierByObject($node->getNodeData()),
                'identifier' => $node->getIdentifier(),
                'dimensions' => $node->getContext()->getDimensions(),
                'workspace' => $node->getWorkspace()->getName(),
                'nodeType' => $node->getNodeType()->getName(),
                'path' => $node->getPath(),
            ];
            try {
                $this->jobManager->queue(self::LIVE_QUEUE_NAME, new IndexingJob(null, $payload));
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

        foreach ($node->findChildNodes() as $childNode) {
            $counter = $this->enqueueTreeRecursively($childNode, $counter, $verbose);
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

    /**
     * Print queue depth counters
     */
    public function statusCommand(): void
    {
        $this->outputLine('<b>%s</b>', [self::LIVE_QUEUE_NAME]);
        try {
            $queue = $this->queueManager->getQueue(self::LIVE_QUEUE_NAME);
            $this->outputLine('  Pending jobs  : %s', [$queue->countReady()]);
            $this->outputLine('  Reserved jobs : %s', [$queue->countReserved()]);
            $this->outputLine('  Failed jobs   : %s', [$queue->countFailed()]);
        } catch (Exception $exception) {
            $this->outputLine('  Queue not available: %s', [$exception->getMessage()]);
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
