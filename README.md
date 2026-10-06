# CodeQ.Meilisearch.QueueIndexer

Asynchronous live indexing for [Medienreaktor.Meilisearch](https://github.com/medienreaktor/Medienreaktor.Meilisearch) using Flowpack JobQueue.

The package decorates Neos' `NodeIndexerInterface` so publish operations enqueue live indexing jobs instead of writing to Meilisearch synchronously. A worker command drains the queue and delegates the actual indexing/removal work back to `Medienreaktor.Meilisearch`.

## Installation

Require the package in the Neos distribution:

```bash
composer require codeq/meilisearch-queueindexer
```

If the package is not available on Packagist yet, add the repository explicitly:

```json
{
    "repositories": {
        "codeq/meilisearch-queueindexer": {
            "type": "vcs",
            "url": "git@github.com:code-q-web-factory/CodeQ.Meilisearch.QueueIndexer.git"
        }
    }
}
```

This package expects `medienreaktor/meilisearch` to be installed and configured by the site package. Projects that use a forked Medienreaktor package must add the corresponding VCS repository in the root `composer.json`.

Version 0.2 currently requires `dev-bugfix/combined-fixes`, which contains the
identifier-based removal API used by deferred removal jobs. This constraint can
be relaxed after that API is included in an upstream release.

## Queue Configuration

The default configuration uses a Doctrine queue named `CodeQ.Meilisearch.QueueIndexer.Live`:

```yaml
Flowpack:
  JobQueue:
    Common:
      queues:
        'CodeQ.Meilisearch.QueueIndexer.Live':
          preset: 'CodeQ.Meilisearch.QueueIndexer.Live'
      presets:
        'CodeQ.Meilisearch.QueueIndexer.Live':
          className: 'Flowpack\JobQueue\Doctrine\Queue\DoctrineQueue'
          options:
            tableName: 'codeq_meilisearch_queueindexer_live'
```

Set up the queue after installing:

```bash
./flow queue:setup 'CodeQ.Meilisearch.QueueIndexer.Live'
```

## Commands

Run the live indexing worker as a daemon. It waits for new jobs and never
returns on its own, so this is the production form (see
[Production Worker](#production-worker)):

```bash
./flow nodeindexqueue:work --verbose
```

Drain the queue once and return - the form to use after a build:

```bash
./flow nodeindexqueue:work --exit-when-empty --verbose
```

`--limit N` is not a drain: it stops after N job *executions*, failed
executions included, and it keeps waiting for new jobs until that many have
run. A limit above the number of queued jobs is therefore never reached and
the worker blocks. Combine it with `--exit-when-empty` if you want both a
work budget and a guaranteed return. Every exit prints what is left in the
queue, so a run that stopped on its budget cannot be mistaken for a finished
drain.

Enqueue all fulltext-root documents from every allowed content-dimension
combination in the live workspace:

```bash
./flow nodeindexqueue:build --verbose
```

Snapshot jobs retain their explicit target dimension until execution. This
matches `nodeindex:build`, including fallback/shine-through variants whose
Meilisearch document hash must represent the requested dimension rather than
the underlying fallback node.

Inspect queue state:

```bash
./flow nodeindexqueue:status
```

The counters are the three states the JobQueue backend stores, labelled with
those state names, and the output also names the backend and its table:

```
CodeQ.Meilisearch.QueueIndexer.Live
  Backend          : Flowpack\JobQueue\Doctrine\Queue\DoctrineQueue
  Table            : codeq_meilisearch_queueindexer_live
  state "ready"    : 3  waiting for a worker, including jobs released for a retry
  state "reserved" : 1  currently held by a worker
  state "failed"   : 2  given up after 4 attempts
  rows in total    : 6
```

Point SQL at the table named there, not at a table from an earlier indexer -
a count taken from a leftover `flowpack_jobqueue_*` table is what makes a
correct report look wrong:

```sql
select state, count(*) from codeq_meilisearch_queueindexer_live group by state;
```

`ready` does not mean "never tried". The JobManager releases a failing job back
to `ready` and increments its failure counter until `maximumNumberOfReleases`
is spent; only then does it become `failed`. A rebuild is applied once all
three counters are zero - not once `failed` looks small.

Flush the live queue:

```bash
./flow nodeindexqueue:flush
```

## Settings

Live async indexing is enabled by default:

```yaml
CodeQ:
  Meilisearch:
    QueueIndexer:
      enableLiveAsyncIndexing: true
```

Set `enableLiveAsyncIndexing: false` to fall back to synchronous Medienreaktor indexing.

## Scheduled Visibility Reconciliation

`Medienreaktor.Meilisearch` keeps scheduled visibility reconciliation disabled
by default. When that upstream feature is enabled, its
`scheduledvisibility:reconcile` command delegates index and removal
operations to `NodeIndexerInterface`. This package's decorator therefore puts
those operations onto the live queue automatically; no second reconciliation
command or queue setting is required here.

The reconciliation command only submits jobs. Keep the worker running to apply
them:

```bash
./flow nodeindexqueue:work --verbose
```

The normal snapshot build remains unchanged. Enabling live asynchronous
indexing does not enable scheduled reconciliation by itself.

New runtime indexing, structural repair and removal jobs persist the fulltext-root aggregate identifier and every affected
dimension combination when they are enqueued. At execution time they replay
`replaceVariants()` against the live workspace, so deleting a content node
refreshes its document and deleting a variant preserves unrelated languages.
The previous root of moved content is captured before publishing, not inferred
again when the worker runs. Repair jobs therefore still refresh it after the
content has moved again or its NodeData has disappeared. Injected services are
explicitly transient and are reinjected by Flow after job deserialization.
Published removals use the live target workspace even when the originating
node still belongs to a user workspace.

The worker explicitly flushes the upstream write buffer after every indexing
and removal job so changes reach Meilisearch immediately.
Before each job, it clears the read-only CR context/node caches and Doctrine
identity map. Extraction evaluates visibility at execution time, not worker
startup time, so later publishes and scheduled boundaries are not read from
an earlier job's snapshot. Restart existing workers after upgrading both packages.

Jobs queued by version 0.2 contain only an immutable document identifier. They
continue to use identifier-based deletion and are flushed immediately. Older
jobs without either immutable payload continue through the legacy
node-rehydration path. Drain the live queue before upgrading. If that cannot be
guaranteed, perform a one-time index flush and complete rebuild after deploying
the new worker so previously orphaned documents cannot remain searchable.

## Index Name Configuration

The queue indexer resolves its own `indexClient` from `Medienreaktor.Meilisearch.indexName`. For a complete environment-specific setup, the consuming site should also configure the upstream Medienreaktor indexer/commands/query builder to use the same setting, for example:

```yaml
Medienreaktor\Meilisearch\Indexer\NodeIndexer:
  properties:
    indexClient:
      object:
        factoryObjectName: Medienreaktor\Meilisearch\Factory\IndexFactory
        factoryMethodName: create
        arguments:
          1:
            setting: 'Medienreaktor.Meilisearch.indexName'
```

## Production Worker

Run `nodeindexqueue:work` continuously in production, without
`--exit-when-empty`: a live worker must keep waiting when the queue is
momentarily empty, or a supervisor would restart it every few seconds on an
idle site. For Beach-style deployments, wrap it in a restart loop so the worker
comes back after PHP exits:

```bash
while true; do
  /application/flow nodeindexqueue:work --verbose
  sleep 10
done
```
