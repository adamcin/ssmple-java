/*
 * Copyright 2018 Mark Adamcin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

class JsonFileStore extends AbstractFileStore {
	private Map<String, String> bindings = new LinkedHashMap<>();

	JsonFileStore(final File file) {
		super(file);
	}

	@Override
	public void load() throws IOException {
		if (getFile().exists()) {
			assertFileIsReadable();
			bindings.clear();
			Map<?, ?> untypedMap = new ObjectMapper().readValue(getFile(), LinkedHashMap.class);
			for (Map.Entry<?, ?> untypedEntry : untypedMap.entrySet()) {
				bindings.put(Objects.toString(untypedEntry.getKey()), Objects.toString(untypedEntry.getValue()));
			}
		}
	}

	@Override
	public void save() throws IOException {
		assertFileIsWritable();
		ObjectMapper mapper = new ObjectMapper();
		mapper.enable(SerializationFeature.INDENT_OUTPUT);
		mapper.writeValue(getFile(), bindings);
	}

	@Override
	public Set<String> getKeys() {
		return bindings.keySet();
	}

	@Override
	public Optional<String> getValue(final String key) {
		return Optional.ofNullable(bindings.get(key));
	}

	@Override
	public void putParam(final String key, final String value) {
		bindings.put(key, value);
	}
}
