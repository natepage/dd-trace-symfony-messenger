<?php
declare(strict_types=1);

use NatePage\DDTrace\SymfonyMessengerIntegration;

if (\extension_loaded('ddtrace') === false) {
    return;
}

(new SymfonyMessengerIntegration())->init();
