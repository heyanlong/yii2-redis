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
        'PING'
    ];

    public $redisWriteCommands = [
        'SET',
    ];

    public $redisReadCommands = [
        'GET',
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
                array_push($this->_socket['slave'], $connection);
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
        if (in_array($redisCommand, $this->redisWriteCommands)) {

            $node = $this->_socket['master'][$this->getMasterNode($params[0])];
            return $this->executeCommand($name, $params, $node);

        } else if (in_array($redisCommand, $this->redisReadCommands)) {

            $node = $this->_socket['master'][$this->getSlaveNode($params[0])];
            return $this->executeCommand($name, $params, $node);

        } else {
            return parent::__call($name, $params);
        }
    }

    public function executeCommand($name, $params = [], $node)
    {
        $this->open();
        $socket = $node;

        array_unshift($params, $name);
        $command = '*' . count($params) . "\r\n";

        foreach ($params as $arg) {
            $command .= '$' . mb_strlen($arg, '8bit') . "\r\n" . $arg . "\r\n";
        }
        \Yii::trace("Executing Redis Command: {$name}", __METHOD__);
        fwrite($socket, $command);

        return $this->parseResponse(implode(' ', $params), $socket);
    }

    private function parseResponse($command, $socket)
    {
        if (($line = fgets($socket)) === false) {
            throw new Exception("Failed to read from socket.\nRedis command was: " . $command);
        }

        $type = $line[0];
        $line = mb_substr($line, 1, -2, '8bit');

        switch ($type) {
            case '+': // Status reply
                if ($line === 'OK' || $line === 'PONG') {
                    return true;
                } else {
                    return $line;
                }
            case '-': // Error reply
                throw new Exception("Redis error: " . $line . "\nRedis command was: " . $command);
            case ':': // Integer reply
                // no cast to int as it is in the range of a signed 64 bit integer
                return $line;
            case '$': // Bulk replies
                if ($line == '-1') {
                    return null;
                }
                $length = $line + 2;
                $data = '';
                while ($length > 0) {
                    if (($block = fread($socket, $length)) === false) {
                        throw new Exception("Failed to read from socket.\nRedis command was: " . $command);
                    }
                    $data .= $block;
                    $length -= mb_strlen($block, '8bit');
                }
                return mb_substr($data, 0, -2, '8bit');
            case '*': // Multi-bulk replies
                $count = (int)$line;
                $data = [];
                for ($i = 0; $i < $count; $i++) {
                    $data[] = $this->parseResponse($command, $socket);
                }
                return $data;
            default:
                throw new Exception('Received illegal data from redis: ' . $line . "\nRedis command was: " . $command);
        }
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

        } else {
            \Yii::error("Failed to open redis DB connection ($connection): $errorNumber - $errorDescription", __CLASS__);
            $message = YII_DEBUG ? "Failed to open redis DB connection ($connection): $errorNumber - $errorDescription" : 'Failed to open DB connection.';
            throw new Exception($message, $errorDescription, (int)$errorNumber);
        }

        return $socket;
    }

    private function disconnect($node)
    {
        //$this->executeCommand('QUIT');
        stream_socket_shutdown($node, STREAM_SHUT_RDWR);
    }

    public function getMasterNode($name)
    {
        return (abs(crc32($name)) % (count($this->_socket['master']))) + 1;
    }

    public function getSlaveNode($name)
    {
        return (abs(crc32($name)) % (count($this->_socket['slave']))) + 1;
    }
}