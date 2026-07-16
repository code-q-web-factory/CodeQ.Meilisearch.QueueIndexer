<?php

declare(strict_types=1);

namespace CodeQ\Meilisearch\QueueIndexer\Indexer;

use CodeQ\Meilisearch\QueueIndexer\Command\NodeIndexQueueCommandController;
use CodeQ\Meilisearch\QueueIndexer\IndexingJob;
use CodeQ\Meilisearch\QueueIndexer\RemovalJob;
use Flowpack\JobQueue\Common\Job\JobManager;
use Medienreaktor\Meilisearch\Indexer\NodeIndexer as UpstreamNodeIndexer;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Log\Utility\LogEnvironment;
use Neos\Flow\Persistence\PersistenceManagerInterface;
use Psr\Log\LoggerInterface;

/**
 * Queueing decorator for Medienreaktor.Meilisearch's NodeIndexer.
 *
 * When `enableLiveAsyncIndexing` is true and the indexing call targets the live
 * workspace, this class enqueues a job instead of writing to Meilisearch directly.
 * Any other path (non-live workspace, async disabled) falls through to the parent
 * implementation so CLI builds and preview workspaces keep their synchronous
 * semantics.
 *
 * @Flow\Scope("singleton")
 */
class NodeIndexer extends UpstreamNodeIndexer
{
    /**
     * @Flow\Inject
     * @var JobManager
     */
    protected $jobManager;

    /**
     * @Flow\Inject
     * @var PersistenceManagerInterface
     */
    protected $persistenceManager;

    /**
     * @Flow\Inject
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * @Flow\InjectConfiguration(package="CodeQ.Meilisearch.QueueIndexer", path="enableLiveAsyncIndexing")
     * @var bool
     */
    protected $enableLiveAsyncIndexing = true;

    /**
     * @param NodeInterface $node
     * @param string|null $targetWorkspace
     * @param bool $indexAllDimensions
     * @param bool $indexFallbackDimensions
     * @param array<string, string[]> $targetDimensionCombination
     */
    public function indexNode(
        NodeInterface $node,
        $targetWorkspace = null,
        $indexAllDimensions = true,
        $indexFallbackDimensions = true,
        array $targetDimensionCombination = []
    ): void {
        if (!$this->shouldEnqueue($node, $targetWorkspace)) {
            $this->indexSynchronously(
                $node,
                $targetWorkspace,
                $indexAllDimensions,
                $indexFallbackDimensions,
                $targetDimensionCombination
            );
            return;
        }

        try {
            $this->jobManager->queue(
                NodeIndexQueueCommandController::LIVE_QUEUE_NAME,
                new IndexingJob($targetWorkspace, $this->nodeAsArray($node))
            );
        } catch (\Throwable $exception) {
            // If the job queue backend is unavailable (e.g. DB hiccup, table missing) we
            // must not break the editor's publish. Log and fall back to synchronous
            // indexing so Meilisearch stays in sync for this publish. If Meilisearch is
            // also down the parent call will raise; the publish signal handler upstream
            // is responsible for deciding what to do in that case.
            $this->logger->warning(
                sprintf('Queueing indexing job failed (%s); falling back to synchronous indexing', $exception->getMessage()),
                LogEnvironment::fromMethodName(__METHOD__)
            );
            $this->indexSynchronously(
                $node,
                $targetWorkspace,
                $indexAllDimensions,
                $indexFallbackDimensions,
                $targetDimensionCombination
            );
        }
    }

    /**
     * Call the concrete parent implementation without re-entering this
     * decorator. The combined-fixes branch accepts a fifth target-dimension
     * argument while released Neos 7/8 upstream versions currently accept four.
     *
     * @param string|null $targetWorkspace
     * @param array<string, string[]> $targetDimensionCombination
     */
    protected function indexSynchronously(
        NodeInterface $node,
        $targetWorkspace,
        bool $indexAllDimensions,
        bool $indexFallbackDimensions,
        array $targetDimensionCombination
    ): void {
        $arguments = [
            $node,
            $targetWorkspace,
            $indexAllDimensions,
            $indexFallbackDimensions,
        ];
        $parentMethod = new \ReflectionMethod(UpstreamNodeIndexer::class, 'indexNode');
        if ($parentMethod->getNumberOfParameters() >= 5) {
            $arguments[] = $targetDimensionCombination;
        }
        $parentMethod->invokeArgs($this, $arguments);
    }

    public function removeNode(NodeInterface $node): void
    {
        if (!$this->shouldEnqueue($node, null)) {
            parent::removeNode($node);
            return;
        }

        try {
            $this->jobManager->queue(
                NodeIndexQueueCommandController::LIVE_QUEUE_NAME,
                new RemovalJob(null, $this->removalNodeAsArray($node))
            );
        } catch (\Throwable $exception) {
            $this->logger->warning(
                sprintf('Queueing removal job failed (%s); falling back to synchronous removal', $exception->getMessage()),
                LogEnvironment::fromMethodName(__METHOD__)
            );
            parent::removeNode($node);
        }
    }

    /**
     * Enqueue only live-workspace changes. Anything else (batch builds over non-live workspaces,
     * draft previews, async disabled) runs synchronously.
     *
     * Matches the gate used by Flowpack.ElasticSearch.ContentRepositoryQueueIndexer so operators
     * can reason about the two the same way.
     */
    protected function shouldEnqueue(NodeInterface $node, ?string $targetWorkspace): bool
    {
        if ($this->enableLiveAsyncIndexing !== true) {
            return false;
        }
        if ($targetWorkspace !== null) {
            return $targetWorkspace === 'live';
        }
        return $node->getContext()->getWorkspaceName() === 'live';
    }

    /**
     * Minimal payload for job serialization.
     *
     * @return array<string, mixed>
     */
    protected function nodeAsArray(NodeInterface $node): array
    {
        return [
            'persistenceObjectIdentifier' => $this->persistenceManager->getIdentifierByObject($node->getNodeData()),
            'identifier' => $node->getIdentifier(),
            'dimensions' => $node->getContext()->getDimensions(),
            'workspace' => $node->getWorkspace()->getName(),
            'nodeType' => $node->getNodeType()->getName(),
            'path' => $node->getPath(),
        ];
    }

    /**
     * Persist the exact document identifier while the NodeData is still
     * available. A deferred removal must not depend on rehydrating the node.
     *
     * @return array<string, mixed>
     */
    protected function removalNodeAsArray(NodeInterface $node): array
    {
        return array_merge($this->nodeAsArray($node), [
            'documentIdentifier' => $this->generateUniqueNodeIdentifier($node),
        ]);
    }
}
