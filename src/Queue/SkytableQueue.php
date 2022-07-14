<?php
/**
 * @since     Jul 2022
 * @author    Haydar KULEKCI <haydarkulekci@gmail.com>
 */

namespace Skytable\Laravel\Queue;

use Illuminate\Contracts\Queue\ClearableQueue;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Skytable\Client;
use Skytable\Laravel\Jobs\SkytableJob;

class SkytableQueue extends Queue implements QueueContract, ClearableQueue
{
    protected Client $client;

    private string $default;

    public function __construct(Client $client, $default = 'skytable')
    {
        $this->client = $client;
        $this->default = $default;
    }

    public function clear($queue)
    {
        $this->client->lmod_clear($queue);
    }

    public function size($queue = null)
    {
        return $this->client->lget_len($this->getQueue($queue));
    }

    public function push($job, $data = '', $queue = null)
    {
        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $this->getQueue($queue), $data),
            $queue,
            null,
            function ($payload, $queue) {
                return $this->pushRaw($payload, $queue);
            }
        );
    }

    /**
     * @return Client
     */
    public function getClient(): Client
    {
        return $this->client;
    }

    /**
     * Get the queue or return the default.
     *
     * @param string|null $queue
     * @return string
     */
    public function getQueue(?string $queue): string
    {
        return ($queue ?: $this->default);
    }

    /**
     * Push a raw payload onto the queue.
     *
     * @param  string  $payload
     * @param  string|null  $queue
     * @param  array  $options
     * @return mixed
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        $this->getClient()->lmod_push($this->getQueue($queue), $payload)->getValue();

        return json_decode($payload, true)['id'] ?? null;
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        throw new \RuntimeException('Not implemented!');
    }

    public function pop($queue = null)
    {
        $job = $this->getClient()->lmod_rpop($this->getQueue($queue))->getValue();

        return new SkytableJob(
            $this->container, $this, $job,
            null, $this->connectionName, $queue ?: $this->default
        );

    }

    public function deleteReserved(string $queue)
    {
        return $this->client->del($this->getQueue($queue).':reserved');
    }
}
