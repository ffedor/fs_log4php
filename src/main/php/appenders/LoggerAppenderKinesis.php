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
 * LoggerAppenderKinesis sends log events to your AWS Kinesis account.
 *
 * This appender uses a layout. TBD (Faber)
 *
 * ## Configurable parameters: ##
 *
 *   AWS_SECRET - read from ~/.aws/credentials
 *   ASW_KEY    - read from ~/.aws/credentials
 *   streamName - read from config file
 *   profile    - read from config file
 *   region     - read from config file
 *   version    - read from config file
 *
 * @version $Revision$
 * @package fs_log4php
 * @subpackage appenders
 * @license http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link http://logging.apache.org/log4php/docs/appenders/Kinesis.html Appender documentation (someday!)
 */

require __DIR__ . "/../../../../../../autoload.php";

use Aws\Kinesis\KinesisClient;


class LoggerAppenderKinesis extends LoggerAppender
{

	/**
	 * The Kinesis streamName to send to .
	 * @var resource
	 */
    protected $streamName;

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
     * The AWS Kinesis API version.
     * @var resource
     */
    protected $version;

	/**
	 * The Kinesis Client.
	 * @var resource
	 */
    private $kinesisClient;

	/**
	 * The list of available shard.
	 * @var resource
	 */
    private $shardIds;

    public function activateOptions() {
        if (empty($this->streamName)) {
            $this->warn("Required parameter 'streamName' not set. Closing appender.");
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

        if (empty($this->version)) {
            $this->warn("Required parameter 'version' not set. Closing appender.");
            $this->closed = true;
            return;
        }

        try {
            $this->kinesisClient = KinesisClient::factory(array(
                    'profile' => $this->profile,
                    'version' => $this->version,
                    'region'  => $this->region)
            );
        } catch (Exception $e) {
            $this->error($e->getMessage());
            return ;
        }

        try {
            $res            = $this->kinesisClient->describeStream(['StreamName' => $this->streamName]);
            $this->shardIds = $res->search('StreamDescription.Shards[].ShardId');
        } catch (Exception $e) {
            $this->error($e->getMessage());
            return ;        }

        if ($this->requiresLayout and ! $this->layout) {
            $this->layout = $this->getDefaultLayout();
        }
    }

    /**
     * Sets the 'streamName' parameter.
     * @param string $streamName
     */
    public function setStreamName($streamName) {
      $this->setString('streamName', $streamName);
    }
    
    /**
     * Returns the 'streamName' parameter.
     * @return string
     */
    public function getStreamName() {
      return $this->streamName;
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

        #TODO Check that $logData is below 1 MiB(?) in size (if it's not, what then?)
        #     Suggestion: chunk the data, assigning a chunk_id
        # On second thought, read this in detail:
        # https://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-sdk.html

        #TODO ? buffer up to 400 messages before actually sending them.

        // this returns a Base 64-encoded JSON string.
        $logData = base64_encode($this->layout->format($event));
        // Get a (specific?) shardId
        $shardId = $this->shardIds[0];

        $record = array("Data"=> $logData,
                        "PartitionKey" => md5($logData));

        $Records[] = $record;

        $parameter = ['StreamName' => $this->streamName, 'Records' => $Records];

        try {
            $res = $this->kinesisClient->putRecords($parameter);
        } catch (Exception $e) {
            return $e->getMessage();
        }

        echo "Failed records: {$res->get('FailedRecordCount')}\n";

    }

}
