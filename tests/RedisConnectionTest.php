<?php
/**
 * Created by PhpStorm.
 * User: heyanlong
 * Date: 2015/5/21
 * Time: 13:58
 */

namespace heyanlong\redis\tests;

class RedisConnectionTest extends TestCase
{
    public function testConnect()
    {
        $db = $this->getConnection(false);
        $database = $db->database;

        $db->open();
        $this->assertTrue($db->ping());
        $db->set('TESTKEY', 'TESTVALUE');
        $db->close();

        $db = $this->getConnection(false);
        $db->database = $database;
        $db->open();
        $this->assertEquals('TESTVALUE', $db->get('TESTKEY'));
        $db->close();
    }
}