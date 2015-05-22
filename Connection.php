<?php
/**
 * @link http://www.heyanlong.com/
 * @copyright Copyright (c) 2015 heyanlong.com
 * @license http://www.heyanlong.com/license/
 */

namespace heyanlong\redis;

use yii\db\Exception;
use yii\helpers\Inflector;

class Connection extends \yii\redis\Connection
{

    const EVENT_AFTER_OPEN = 'afterOpen';

    public $master = [];

    private $_socket;

    public function __sleep()
    {
        $this->close();

        return array_keys(get_object_vars($this));
    }

    public function getIsActive()
    {
        return $this->_socket !== null;
    }

    public function open($hostname = '')
    {
        if ($this->getIsActive() && $hostname == '') {
            return;
        }

        $hostname = $hostname == '' ? $this->master[array_rand($this->master)] : $hostname;
        $this->_socket = $this->connect($hostname);
    }

    public function close()
    {
        if ($this->_socket !== null) {
            $this->executeCommand('QUIT');
            stream_socket_shutdown($this->_socket, STREAM_SHUT_RDWR);
            $this->_socket = null;
        }
    }

    protected function initConnection()
    {
        $this->trigger(self::EVENT_AFTER_OPEN);
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

    public function executeCommand($name, $params = [], $hostname = '')
    {
        $this->open($hostname);

        array_unshift($params, $name);
        $command = '*' . count($params) . "\r\n";

        foreach ($params as $arg) {
            $command .= '$' . mb_strlen($arg, '8bit') . "\r\n" . $arg . "\r\n";
        }
        \Yii::trace("Executing Redis Command: {$name}", __METHOD__);
        fwrite($this->_socket, $command);

        return $this->parseResponse(implode(' ', $params), $this->_socket);
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

                $moved = explode(' ', $line);
                if (isset($moved[0]) && $moved[0] == 'MOVED') {

                    $hostname = $moved[2];

                    $name = explode(' ', $command)[0];
                    $param = array_slice(explode(' ', $command), 1);

                    return $this->executeCommand($name, $param, $hostname);

                } else {
                    throw new Exception("Redis error: " . $line . "\nRedis command was: " . $command);
                }

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
}