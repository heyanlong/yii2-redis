<?php

/**
 * This is the configuration file for the Yii2 unit tests.
 * You can override configuration values by creating a `config.local.php` file
 * and manipulate the `$config` variable.
 * For example to change MySQL username and password your `config.local.php` should
 * contain the following:
 *
 */

$config = [
    'databases' => [
        'redis' => [
            'master' => [
//                '10.155.20.167:6391',
//                '10.155.20.168:6379',
//                '10.155.20.167:6380',
//                '10.155.20.169:6379',
                'localhost:6379',
            ],
            'slave' => [
//                '10.155.20.167:6379',
//                '10.155.20.168:6380',
//                '10.155.20.168:6391',
//                '10.155.20.169:6380',
                'localhost:6379',
            ],
            'database' => 0,
            'password' => null,
        ],
    ],
];

return $config;