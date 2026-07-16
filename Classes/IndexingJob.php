<?php

declare(strict_types=1);

namespace CodeQ\Meilisearch\QueueIndexer;

use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Neos\ContentRepository\Domain\Model\NodeData;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Neos\Flow\Log\Utility\LogEnvironment;

/**
 * Executes a single deferred `indexNode` call against Medienreaktor.Meilisearch.
 */
class IndexingJob extends AbstractIndexingJob
{
    /**
     * Keep these defaults for live-publish jobs and jobs serialized by package
     * versions before dimension-aware snapshot builds were introduced.
     *
     * @var bool
     */
    protected $indexAllDimensions = true;

    /** @var bool */
    protected $indexFallbackDimensions = true;

    /** @var array<string, string[]> */
    protected $targetDimensionCombination = [];

    /**
     * @param array<string, mixed> $node
     * @param array<string, string[]> $targetDimensionCombination
     * @throws \Exception
     */
    public function __construct(
        ?string $targetWorkspaceName,
        array $node,
        bool $indexAllDimensions = true,
        bool $indexFallbackDimensions = true,
        array $targetDimensionCombination = []
    ) {
        parent::__construct($targetWorkspaceName, $node);
        $this->indexAllDimensions = $indexAllDimensions;
        $this->indexFallbackDimensions = $indexFallbackDimensions;
        $this->targetDimensionCombination = $targetDimensionCombination;
    }

    public function execute(QueueInterface $queue, Message $message): bool
    {
        /** @var NodeData $nodeData */
        $nodeData = $this->nodeDataRepository->findByIdentifier($this->node['persistenceObjectIdentifier']);
        if (!$nodeData instanceof NodeData) {
            // The underlying NodeData can disappear between enqueue and execute - e.g. the node was
            // deleted. Skip instead of failing the job; a RemovalJob covers the delete path.
            $this->logger->notice(
                sprintf('NodeData for node %s not found, skipping indexing job', $this->node['identifier']),
                LogEnvironment::fromMethodName(__METHOD__)
            );
            return true;
        }

        $dimensions = $this->targetDimensionCombination !== []
            ? $this->targetDimensionCombination
            : $this->node['dimensions'];
        $context = $this->contextFactory->create([
            'workspaceName' => $this->targetWorkspaceName ?: $nodeData->getWorkspace()->getName(),
            'invisibleContentShown' => true,
            'inaccessibleContentShown' => false,
            'dimensions' => $dimensions,
        ]);

        $node = $this->nodeFactory->createFromNodeData($nodeData, $context);
        if (!$node instanceof NodeInterface) {
            $this->logger->warning(
                sprintf('Node %s could not be rehydrated for indexing', $this->node['identifier']),
                LogEnvironment::fromMethodName(__METHOD__)
            );
            return true;
        }

        $this->nodeIndexer->indexNode(
            $node,
            $this->targetWorkspaceName,
            $this->indexAllDimensions,
            $this->indexFallbackDimensions,
            $this->targetDimensionCombination
        );
        return true;
    }

    public function getLabel(): string
    {
        return sprintf('Meilisearch Indexing Job (%s)', $this->getIdentifier());
    }
}
