<?php
/**
 * @since     Jul 2022
 * @author    Haydar KULEKCI <haydarkulekci@gmail.com>
 */

namespace Skytable\Laravel\Connectors;

use Illuminate\Queue\Connectors\ConnectorInterface;
use Skytable\Client;
use Skytable\Laravel\Queue\SkytableQueue;

class SkytableConnector implements ConnectorInterface
{

    /**
     * The Redis database instance.
     *
     * @var Client
     */
    protected Client $skytable;

    /**
     * @return void
     */
    public function __construct(Client $skytable)
    {
        $this->skytable = $skytable;
    }

    public function connect(array $config): SkytableQueue
    {
        return new SkytableQueue($this->skytable, $config['queue'] ?? 'skytable');
    }
}
