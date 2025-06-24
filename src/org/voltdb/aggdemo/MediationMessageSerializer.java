/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package org.voltdb.aggdemo;


import org.apache.kafka.common.serialization.Serializer;

/**
 * Used by Kafka to serialize MediationMessage objects.
 *
 */
public class MediationMessageSerializer implements Serializer<MediationMessage> {

	@Override
	public byte[] serialize(String topic, MediationMessage data) {
		
		return data.toString().getBytes();
	}

}
