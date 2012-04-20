<?php
/**
 * Riak Session Handler for PHP
 * 
 * PHP version 5.3
 * 
 * Copyright 2011 Zachary Fox
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT 
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the 
 * License for the specific language governing permissions and limitations 
 * under the License.
 * 
 * @category SessionHandlers
 * @package  RiakSession
 * @author   Zachary Fox <zachreligious@gmail.com>
 * @license  http://www.opensource.org/licenses/Apache-2.0 Apache 2.0 License
 * @link     http://github.com/zacharyfox/riak-php-session
 */

/**
 * The Riak Session handler for PHP allows you to use a RIAK cluster to store
 * session information using PHP native sessions. Requires riak-php-client
 * 
 * 
 * @category SessionHandlers
 * @package  RiakSession
 * @author   Zachary Fox <zachreligious@gmail.com>
 * @license  http://www.opensource.org/licenses/Apache-2.0 Apache 2.0 License
 * @link     http://github.com/zacharyfox/riak-php-session
 */
class RiakSession
{
    /**
     * @var string Hostname or IP of Riak server
     */
    const HOST   = "127.0.0.1";
    
    /**
     * @var integer Port number of Riak server
     */
    const PORT   = 8091;
    
    /**
     * @var string Riak bucket name to store sessions
     */
    const BUCKET = "sessions";
    
    /**
     * @var boolean Store PHP session data as json
     */
    const JSON = true;
    
    /**
     * @var RiakClient
     */
    private $_client;
    
    /**
     * @var RiakBucket
     */
    private $_bucket;
    
    /**
     * @var RiakObject
     */
    private $_session;
    
    /**
     * @var array Default configuration values
     */
    private $_settings = array(
        'host'   => self::HOST,
        'port'   => self::PORT,
        'bucket' => self::BUCKET,
        'json'   => self::JSON
    );
    
    /**
     * Create a new session handler instance
     * 
     * @param array $options Configuration options
     * 
     * @return RiakSession
     */
    public function __construct($options = array())
    {
        $this->_settings = array_replace($this->_settings, $options);
        
        $this->_client = new RiakClient(
            $this->_settings['host'],
            $this->_settings['port']
        );
        
        $this->_bucket = new RiakBucket(
            $this->_client,
            $this->_settings['bucket']
        );
        
        if ($this->_settings['json'] === true) {
            ini_set('session.serialize_handler', 'wddx');
        }
        
        session_set_save_handler(
            array($this, "open"),
            array($this, "close"),
            array($this, "read"),
            array($this, "write"),
            array($this, "destroy"),
            array($this, "gc")
        );
        
        register_shutdown_function('session_write_close');
    }
    
    /**
     * Open the session
     *
     * @param string $savePath    session.save_path value
     * @param string $sessionName session.name value
     * 
     * @return boolean
     */
    public function open($savePath, $sessionName)
    {
        return $this->_bucket instanceof RiakBucket;
    }
    
    /**
     * Close the session
     *
     * @return boolean
     */
    public function close()
    {
        return true;
    }
    
    /**
     * Read the session data
     *
     * @param string $id Session ID
     * 
     * @return string Serialized Session Data
     */
    public function read($id)
    {
        $this->_session($id)->data['atime'] = time();
        $this->_session = $this->_session($id)->store();
        
        $sessionData = $this->_session($id)->data['data'];
        
        if (empty($sessionData)) {
            return '';
        }
        
        if ($this->_settings['json'] === true) {
            $sessionData = wddx_serialize_value(
                json_decode($sessionData, true)
            );
        }
        
        return $sessionData;
    }
    
    /**
     * Write the session data
     *
     * @param string $id          Session ID
     * @param string $sessionData Serialized Session Data
     * 
     * @return boolean
     */
    public function write($id, $sessionData)
    {
        if ($this->_settings['json'] === true) {
            $sessionData = json_encode(wddx_deserialize($sessionData));
        }
        
        $data = array(
            'data'  => $sessionData,
            'atime' => time()
        );
        
        $this->_session($id)->setData($data);
        
        return ($this->_session($id)->store() instanceof RiakObject);
    }
    
    /**
     * Destroy the session
     * 
     * @param string $id Session ID
     * 
     * @return boolean
     */
    public function destroy($id)
    {
        $this->_session($id)->delete();
        
        return true;
    }
    
    /**
     * Garbage collector
     * 
     * This runs a map on the cluster to get the keys of sessions that haven't
     * been modified in $maxLifetime seconds and then deletes them.
     *
     * @param integer $maxLifetime session.gc_maxlifetime value
     * 
     * @return boolean
     */
    public function gc($maxLifetime)
    {
        $arg = time() - $maxLifetime;
        
        $function = "
        function(value, keyData, arg) {
            var data = Riak.mapValuesJson(value)[0];
            if (data.atime == undefined
                || data.data == undefined
                || data.atime < arg) {
                return [value.key];
            }
            return [];
          }";
        
        $result = $this->_client->add($this->_settings['bucket'])
            ->map($function, array('arg' => $arg, 'keep' => true))
            ->run();
        
        foreach ($result as $id) {
            $this->destroy($id);
        }
        
        return true;
    }
    
    /**
     * Get the RiakObject
     *
     * @param string $id Session ID
     * 
     * @return RiakObject
     */
    private function _session($id)
    {
        if (!($this->_session instanceof RiakObject)) {
            $this->_session = $this->_bucket->get($id);
            // Create a new session if it doesn't exist
            if (!$this->_session->exists()) {
                $data = array(
                    'data'  => '',
                    'atime' => time()
                );
                $this->_session = $this->_bucket
                    ->newObject($id, $data)
                    ->store();
            }
        }
        
        return $this->_session;
    }
}