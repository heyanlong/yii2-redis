<?php
/**
 * @link http://www.heyanlong.com/
 * @copyright Copyright (c) 2015 heyanlong.com
 * @license http://www.heyanlong.com/license/
 */

namespace heyanlong\redis;

use yii\base\Component;
use yii\db\Exception;
use yii\helpers\Inflector;

class Connection extends Component
{

    const EVENT_AFTER_OPEN = 'afterOpen';

    public $master = [];

    public $slave = [];

    public $database = 0;

    public $connectionTimeout = null;

    public $dataTimeout = null;

    public $password = null;

    private $_socket = ['master' => [], 'slave' => []];


    public $redisCommands = [

    ];

    public function __sleep()
    {
        $this->close();

        return array_keys(get_object_vars($this));
    }

    public function getIsActive()
    {
        return count($this->_socket['master']) > 0 && count($this->_socket['slave']) > 0;
    }

    public function open()
    {
        if ($this->getIsActive()) {
            return;
        }

        if (!empty($this->master)) {
            foreach ($this->master as $node) {
                $connection = $this->connect($node);
                array_push($this->_socket['master'], $connection);
            }
        }

        if (!empty($this->slave)) {
            foreach ($this->slave as $node) {
                $connection = $this->connect($node);
                array_push($this->_socket['master'], $connection);
            }
        }
    }

    public function close()
    {
        if (!empty($this->_socket['master'])) {
            foreach ($this->_socket['master'] as $node) {
                $this->disconnect($node);
            }
        }

        if (!empty($this->_socket['slave'])) {
            foreach ($this->_socket['slave'] as $node) {
                $this->disconnect($node);
            }
        }

        $this->_socket = ['master' => [], 'slave' => []];
    }

    protected function initConnection()
    {
        $this->trigger(self::EVENT_AFTER_OPEN);
    }

    public function getDriverName()
    {
        return 'redis';
    }

    public function getLuaScriptBuilder()
    {
        // TODO
    }

    public function __call($name, $params)
    {
        $redisCommand = strtoupper(Inflector::camel2words($name, false));
        if (in_array($redisCommand, $this->redisCommands)) {
            return $this->executeCommand($name, $params);
        } else {
            return parent::__call($name, $params);
        }
    }

    public function executeCommand($name, $params = [])
    {
        $this->open();

        // TODO
    }

    private function parseResponse($command)
    {
        // TODO
    }

    private function connect($node)
    {
        $socket = null;

        $connection = $node . ', database=' . $this->database;
        \Yii::trace('Opening redis DB connection: ' . $connection, __METHOD__);

        $socket = stream_socket_client(
            'tcp://' . $connection,
            $errorNumber,
            $errorDescription,
            $this->connectionTimeout ? $this->connectionTimeout : ini_get("default_socket_timeout")
        );

        if ($socket) {
            if ($this->dataTimeout !== null) {
                stream_set_timeout($socket, $timeout = (int)$this->dataTimeout, (int)(($this->dataTimeout - $timeout) * 1000000));
            }

            if ($this->password !== null) {
                $this->executeCommand('AUTH', [$this->password]);
            }

            $this->executeCommand('SELECT', [$this->database]);
            $this->initConnection();

        } else {
            \Yii::error("Failed to open redis DB connection ($connection): $errorNumber - $errorDescription", __CLASS__);
            $message = YII_DEBUG ? "Failed to open redis DB connection ($connection): $errorNumber - $errorDescription" : 'Failed to open DB connection.';
            throw new Exception($message, $errorDescription, (int)$errorNumber);
        }
    }

    private function disconnect($node)
    {
        $this->executeCommand('QUIT');
        stream_socket_shutdown($node, STREAM_SHUT_RDWR);
    }
}