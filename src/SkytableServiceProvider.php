<?php
/**
 * @since     Jul 2022
 * @author    Haydar KULEKCI <haydarkulekci@gmail.com>
 */

namespace Skytable\Laravel;

use Illuminate\Contracts\Support\DeferrableProvider;
use Illuminate\Support\ServiceProvider;
use Skytable\Client;
use Skytable\Connection;
use Skytable\Laravel\Connectors\SkytableConnector;

class SkytableServiceProvider extends ServiceProvider implements DeferrableProvider
{
    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register(): void
    {
        $this->app->singleton('skytable.connection', function ($app) {
            $config = $app->make('config')->get('database.skytable', []);
            return new Connection($config['host'], $config['port'], static function ($con) use ($config) {
                $skytable = new Client($con);
                $skytable->create_table($config['database']. ':' . $config['table'], 'keymap(str,list<str>)');
                $skytable->select($config['database']. ':' . $config['table']);
                $skytable->lset('skytable', []);
            });
        });

        $this->app->singleton('skytable', function ($app) {
            return new Client($app['skytable.connection']);
        });

        $this->app['queue']->addConnector('skytable', function () {
            return new SkytableConnector($this->app['skytable']);
        });
    }

    /**
     * Get the services provided by the provider.
     *
     * @return array
     */
    public function provides(): array
    {
        return ['skytable', 'skytable.connection'];
    }
}
