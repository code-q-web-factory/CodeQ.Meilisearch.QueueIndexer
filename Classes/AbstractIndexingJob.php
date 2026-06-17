<?php

declare(strict_types=1);

namespace CodeQ\Meilisearch\QueueIndexer;

use Flowpack\JobQueue\Common\Job\JobInterface;
use Medienreaktor\Meilisearch\Indexer\NodeIndexer;
use Neos\ContentRepository\Domain\Factory\NodeFactory;
use Neos\ContentRepository\Domain\Repository\NodeDataRepository;
use Neos\ContentRepository\Domain\Service\ContextFactoryInterface;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Utility\Algorithms;
use Psr\Log\LoggerInterface;

/**
 * Shared state for IndexingJob and RemovalJob.
 *
 * Payload carries only what is needed to rehydrate the node at execution
 * time. The actual indexing call is delegated to the upstream (non-queued)
 * NodeIndexer; dependency injection resolves that class to the parent rather
 * than this package's override (see Objects.yaml in CodeQ.Search).
 */
abstract class AbstractIndexingJob implements JobInterface
{
    /**
     * @Flow\Inject
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * @Flow\Inject
     * @var NodeIndexer
     */
    protected $nodeIndexer;

    /**
     * @Flow\Inject
     * @var NodeDataRepository
     */
    protected $nodeDataRepository;

    /**
     * @Flow\Inject
     * @var NodeFactory
     */
    protected $nodeFactory;

    /**
     * @Flow\Inject
     * @var ContextFactoryInterface
     */
    protected $contextFactory;

    /**
     * @var string
     */
    protected $identifier;

    /**
     * @var string|null
     */
    protected $targetWorkspaceName;

    /**
     * Minimal payload: persistenceObjectIdentifier, identifier, dimensions, workspace, nodeType, path.
     *
     * @var array
     */
    protected $node;

    /**
     * @param string|null $targetWorkspaceName
     * @param array $node
     * @throws \Exception
     */
    public function __construct(?string $targetWorkspaceName, array $node)
    {
        $this->identifier = Algorithms::generateRandomString(24);
        $this->targetWorkspaceName = $targetWorkspaceName;
        $this->node = $node;
    }

    public function getIdentifier(): string
    {
        return $this->identifier;
    }
}
