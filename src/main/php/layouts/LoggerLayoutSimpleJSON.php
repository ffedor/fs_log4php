<?php
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @package fs_log4php
 */

# TODO Update this comment block
/**
 * A simple JSON layout.
 *
 * Returns the log statement in a format consisting of the
 *  { "timestamp:"$timestamp", "level": "$level", "message":"$message" ] 
 *
 * @version $Revision$
 * @package fs_log4php
 * @subpackage layouts
 */  
class LoggerLayoutSimpleJSON extends LoggerLayout {
	/**
	 * Returns the the message in the log statement as an
	 *
	 * @param LoggerLoggingEvent $event
	 * @return JSON object
	 */
	public function format(LoggerLoggingEvent $event) {
        $timeStamp = $event->timeStamp;
        $level     = $event->getLevel();
        $message   = $event->getRenderedMessage();

        // turn the message into an associative array
        $json = json_decode($message, true);

		return json_encode($json);
	}
}
