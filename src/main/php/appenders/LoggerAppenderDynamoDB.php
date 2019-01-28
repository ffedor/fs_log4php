<?php
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * LoggerAppenderDynamoDB sends log events to your AWS DynamoDB account.
 *
 * This appender uses a layout. TBD (Faber)
 *
 * ## Configurable parameters: ##
 *
 *   AWS_SECRET - read from ~/.aws/credentials
 *   ASW_KEY    - read from ~/.aws/credentials
 *   tableName  - read from config file
 *   profile    - read from config file
 *   region     - read from config file
 *   version    - read from config file
 *
 * @version $Revision$
 * @package log4php
 * @subpackage appenders
 * @license http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link http://logging.apache.org/log4php/docs/appenders/DynamoDB.html Appender documentation (someday!)
 */

require __DIR__ . "/../../../../../../autoload.php";

use Aws\DynamoDB\DynamoDBClient;
use Aws\DynamoDb\Marshaler;

class LoggerAppenderDynamoDB extends LoggerAppender
{

	/**
	 * The DynamoDB tableName to send to .
	 * @var resource
	 */
    protected $tableName;

    /**
     * The AWS profile.
     * @var resource
     */
    protected $profile;

    /**
     * The AWS region.
     * @var resource
     */
    protected $region;

    /**
     * The URL of the DynamoDB instance.
     * @var resource
     */
    protected $endpoint;

    /**
     * The AWS DynamoDB API version.
     * @var resource
     */
    protected $version;

	/**
	 * The DynamoDB Client.
	 * @var resource
	 */
    private $DynamoDBClient;

    public function activateOptions() {
        if (empty($this->tableName)) {
            $this->warn("Required parameter 'tableName' not set. Closing appender.");
            $this->closed = true;
            return;
        }
        if (empty($this->profile)) {
            $this->warn("Required parameter 'profile' not set. Closing appender.");
            $this->closed = true;
            return;
        }
        if (empty($this->region)) {
            $this->warn("Required parameter 'region' not set. Closing appender.");
            $this->closed = true;
            return;
        }
        // This is only required for DynamoDB Local
/*        if (empty($this->endpoint)) {
            $this->warn("Required parameter 'endpoint' not set. Closing appender.");
            $this->closed = true;
            return;
        }*/
        if (empty($this->version)) {
            $this->warn("Required parameter 'version' not set. Closing appender.");
            $this->closed = true;
            return;
        }

        $config = array('profile'  => $this->profile,
                        'region'   => $this->region,
                        'version'  => $this->version);

        if (! empty($this->endpoint)){
            $config['endpoint'] = $this->endpoint;
        }

        try {
            $this->DynamoDBClient = DynamoDBClient::factory($config);
        } catch (Exception $e) {
            $this->error($e->getMessage());
            return ;
        }

/*
        try {
            $res            = $this->DynamoDBClient->describeStream(['tableName' => $this->tableName]);
            $this->shardIds = $res->search('StreamDescription.Shards[].ShardId');
        } catch (Exception $e) {
            $this->error($e->getMessage());
            return ;        }
*/

        if ($this->requiresLayout and ! $this->layout) {
            $this->layout = $this->getDefaultLayout();
        }
    }

    /**
     * Sets the 'tableName' parameter.
     * @param string $tableName
     */
    public function setTableName($tableName) {
      $this->setString('tableName', $tableName);
    }
    
    /**
     * Returns the 'tableName' parameter.
     * @return string
     */
    public function getTableName() {
      return $this->tableName;
    }

    /**
     * Sets the 'profile' parameter.
     * @param string $profile
     */
    public function setProfile($profile) {
        $this->setString('profile', $profile);
    }

    /**
     * Returns the 'profile' parameter.
     * @return string
     */
    public function getProfile() {
        return $this->profile;
    }

    /**
     * Sets the 'region' parameter.
     * @param string $region
     */
    public function setRegion($region) {
        $this->setString('region', $region);
    }

    /**
     * Returns the 'region' parameter.
     * @return string
     */
    public function getRegion() {
        return $this->region;
    }

    /**
     * Sets the 'region' parameter.
     * @param string $region
     */
    public function setEndpoint($endpoint) {
        $this->setString('endpoint', $endpoint);
    }

    /**
     * Returns the 'region' parameter.
     * @return string
     */
    public function getEndpoint() {
        return $this->endpoint;
    }
    /**
     * Sets the 'version' parameter.
     * @param string $version
     */
    public function setVersion($version) {
        $this->setString('version', $version);
    }

    /**
     * Returns the 'version' parameter.
     * @return string
     */
    public function getVersion() {
        return $this->version;
    }

    public function append(LoggerLoggingEvent $event)
    {
        $res = '';

        try {
            $logData = $this->layout->format($event);
            $marshaler = new Marshaler();

            $res = $this->DynamoDBClient->putItem(array(
                                                    'TableName' => $this->tableName,
                                                    'Item'      => $marshaler->marshalJson($logData))
                                                  );
        } catch (Exception $e) {
            if(is_object($res)) {
                echo "Failed records: {$res->get('FailedRecordCount')}\n";
            }
            error_log($e->getMessage() . PHP_EOL, 3, "/tmp/my-errors.log");
            return $e->getMessage();
        }
    }
}
