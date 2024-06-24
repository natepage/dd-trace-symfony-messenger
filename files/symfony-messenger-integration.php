<?php
declare(strict_types=1);

use DDTrace\HookData;
use DDTrace\SpanData;
use DDTrace\SpanLink;
use DDTrace\Tag;
use DDTrace\Util\ObjectKVStore;
use NatePage\DDTrace\DDTraceStamp;
use Symfony\Component\Messenger\Bridge\AmazonSqs\Transport\AmazonSqsReceivedStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\BusNameStamp;
use Symfony\Component\Messenger\Stamp\ConsumedByWorkerStamp;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\HandledStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;

use function DDTrace\active_span;
use function DDTrace\close_span;
use function DDTrace\consume_distributed_tracing_headers;
use function DDTrace\generate_distributed_tracing_headers;
use function DDTrace\install_hook;
use function DDTrace\logs_correlation_trace_id;
use function DDTrace\set_distributed_tracing_context;
use function DDTrace\start_trace_span;
use function DDTrace\trace_method;

if (\extension_loaded('ddtrace') === false) {
    return;
}

/* ---- Helper Functions ---- */
function setSpanAttributes(
    SpanData $span,
    $transportName = null,
    $envelope = null,
    $throwable = null,
    $useMessageAsResource = null,
    $operation = null
) {
    // Set defaults
    $operation = $operation ?? 'dispatch';
    $messageName = \is_object($envelope) ? \get_class($envelope) : null;
    $resource = null;

    if ($envelope instanceof Envelope) {
        $consumedByWorkerStamp = $envelope->last(ConsumedByWorkerStamp::class);
        $receivedStamp = $envelope->last(ReceivedStamp::class);
        $handledStamp = $envelope->last(HandledStamp::class);

        $messageName = \get_class($envelope->getMessage());
        $transportName = $receivedStamp ? $receivedStamp->getTransportName() : $transportName;

        if ($consumedByWorkerStamp || $receivedStamp) {
            $operation = 'receive';
        }

        if ($handledStamp) {
            $resource = $handledStamp->getHandlerName();
        }

        $span->meta = \array_merge($span->meta, resolveMetadataFromEnvelope($envelope));
    }

    if ($span->name === null || $span->name === '') {
        $span->name = \sprintf('symfony_messenger.%s', $operation);
    }

    $span->resource = $resource;
    $span->service = \ddtrace_config_app_name('symfony');
    $span->type = 'queue';
    $span->meta[Tag::SPAN_KIND] = 'client';
    $span->meta[Tag::COMPONENT] = 'symfonymessenger';
    $span->meta[Tag::MQ_OPERATION] = $operation;

    if (($useMessageAsResource ?? false) && $resource === null) {
        $span->resource = $transportName !== null && $transportName !== ''
            ? \sprintf('%s -> %s', $messageName, $transportName)
            : $messageName;
    }

    if ($throwable instanceof \Throwable) {
        $span->exception = $throwable;
    }
}

function resolveMetadataFromEnvelope(Envelope $envelope): array
{
    $busStamp = $envelope->last(BusNameStamp::class);
    $delayStamp = $envelope->last(DelayStamp::class);
    $handledStamp = $envelope->last(HandledStamp::class);
    $receivedStamp = $envelope->last(ReceivedStamp::class);
    $redeliveryStamp = $envelope->last(RedeliveryStamp::class);
    $transportMessageIdStamp = $envelope->last(TransportMessageIdStamp::class);

    $messageName = \get_class($envelope->getMessage());
    $transportName = $receivedStamp ? $receivedStamp->getTransportName() : null;
    $transportMessageId = $transportMessageIdStamp ? $transportMessageIdStamp->getId() : null;

    // AWS SQS
    if (\class_exists(AmazonSqsReceivedStamp::class)) {
        $amazonSqsReceivedStamp = $envelope->last(AmazonSqsReceivedStamp::class);
        $transportMessageId = $amazonSqsReceivedStamp ? $amazonSqsReceivedStamp->getId() : null;
    }

    $stamps = [];
    foreach ($envelope->all() as $stampFqcn => $instances) {
        $stamps[$stampFqcn] = \count($instances);
    }

    $metadata = [
        'messaging.symfony.bus' => $busStamp ? $busStamp->getBusName() : null,
        'messaging.symfony.name' => $messageName,
        'messaging.symfony.transport' => $transportName,
        'messaging.symfony.handler' => $handledStamp ? $handledStamp->getHandlerName() : null,
        'messaging.symfony.delay' => $delayStamp ? $delayStamp->getDelay() : null,
        'messaging.symfony.retry_count' => $redeliveryStamp ? $redeliveryStamp->getRetryCount() : null,
        'messaging.symfony.redelivered_at' => $redeliveryStamp ? $redeliveryStamp->getRedeliveredAt()->format('Y-m-d\TH:i:sP') : null,
        'messaging.symfony.stamps' => $stamps,
        Tag::MQ_DESTINATION => $transportName,
        Tag::MQ_SYSTEM => 'symfony',
        Tag::MQ_DESTINATION_KIND => 'queue',
        Tag::MQ_MESSAGE_ID => $transportMessageId,
    ];

    return \array_filter($metadata, function ($value): bool {
        if (\is_array($value)) {
            return \count($value) > 0;
        }

        return $value !== null && $value !== '';
    });
}

/* ---- Tracing Functions ---- */
trace_method(
    'Symfony\Component\Messenger\MessageBusInterface',
    'dispatch',
    function (SpanData $span, array $args, $returnValue, $exception = null) {
        setSpanAttributes(
            $span,
            null,
            $args[0],
            $exception
        );
    }
);

// Attach current context to Envelope before sender sends it to remote queue
install_hook(
    'Symfony\Component\Messenger\Transport\Sender\SenderInterface::send',
    function (HookData $hook) {
        /** @var \Symfony\Component\Messenger\Envelope $envelope */
        $envelope = $hook->args[0];

        if (\ddtrace_config_distributed_tracing_enabled()) {
            $ddTraceStamp = $envelope->last(DDTraceStamp::class);

            // Add distributed tracing stamp only if not already on the envelope
            if ($ddTraceStamp === null) {
                $hook->overrideArguments([
                    $envelope->with(new DDTraceStamp(generate_distributed_tracing_headers()))
                ]);
            }
        }
    }
);

trace_method(
    'Symfony\Component\Messenger\Worker',
    'handleMessage',
    [
        'prehook' => function (SpanData $span, array $args) use (&$newTrace) {
            /** @var \Symfony\Component\Messenger\Envelope $envelope */
            $envelope = $args[0];
            /** @var string $transportName */
            $transportName = $args[1];

            setSpanAttributes(
                $span,
                $transportName,
                $envelope,
                null,
                true,
                'receive' // Override operation to receive as envelope doesn't have right stamps yet
            );

            $ddTraceStamp = $envelope->last(DDTraceStamp::class);
            if ($ddTraceStamp instanceof DDTraceStamp) {
                if (\dd_trace_env_config('DD_TRACE_LARAVEL_QUEUE_DISTRIBUTED_TRACING')) {
                    $newTrace = start_trace_span();
                    setSpanAttributes(
                        $newTrace,
                        $transportName,
                        $envelope,
                        null,
                        true
                    );

                    consume_distributed_tracing_headers($ddTraceStamp->getHeaders());

                    $span->links[] = $newTrace->getLink();
                    $newTrace->links[] = $span->getLink();
                } else {
                    $span->links[] = SpanLink::fromHeaders($ddTraceStamp->getHeaders());
                }
            }
        },
        'posthook' => function (SpanData $span, array $args, $returnValue, $exception = null) use (&$newTrace) {
            /** @var \Symfony\Component\Messenger\Envelope $envelope */
            $envelope = $args[0];
            /** @var string $transportName */
            $transportName = $args[1];

            if ($exception !== null) {
                // Used by Logs Correlation to track the origin of an exception
                ObjectKVStore::put(
                    $exception,
                    'exception_trace_identifiers',
                    [
                        'trace_id' => logs_correlation_trace_id(),
                        'span_id' => \dd_trace_peek_span_id()
                    ]
                );
            }

            $activeSpan = active_span();
            if (dd_trace_env_config('DD_TRACE_LARAVEL_QUEUE_DISTRIBUTED_TRACING')
                && $activeSpan !== $span
                && $activeSpan === $newTrace) {
                setSpanAttributes(
                    $activeSpan,
                    $transportName,
                    $envelope,
                    $exception,
                    true
                );

                close_span();

                if (
                    dd_trace_env_config("DD_TRACE_REMOVE_ROOT_SPAN_LARAVEL_QUEUE")
                    && dd_trace_env_config("DD_TRACE_REMOVE_AUTOINSTRUMENTATION_ORPHANS")
                ) {
                    set_distributed_tracing_context("0", "0");
                }
            }

            setSpanAttributes(
                $span,
                $transportName,
                $envelope,
                $exception,
                true
            );
        },
        'recurse' => true,
    ]
);

trace_method(
    'Symfony\Component\Messenger\Middleware\MiddlewareInterface',
    'handle',
    function (SpanData $span, array $args, $returnValue, $exception = null) {
        setSpanAttributes(
            $span,
            null,
            $args[0],
            $exception
        );
    }
);

// Since Symfony 6.2
//trace_method(
//    'Symfony\Component\Messenger\Middleware\HandleMessageMiddleware',
//    'callHandler',
//    function (SpanData $span, array $args, $returnValue, $exception = null) {
//        setSpanAttributes(
//            $span,
//            null,
//            null,
//            $exception
//        );
//    }
//);
