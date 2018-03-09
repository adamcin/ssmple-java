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

package net.adamcin.ssmple;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

class YamlFileStore extends AbstractFileStore {
	private final Map<String, String> bindings = new LinkedHashMap<>();

	private final Yaml yaml;

	YamlFileStore(final File file) {
		super(file);
		DumperOptions dumperOptions = new DumperOptions();
		dumperOptions.setPrettyFlow(true);
		dumperOptions.setExplicitStart(true);
		dumperOptions.setExplicitEnd(true);
		this.yaml = new Yaml(dumperOptions);
	}

	@Override
	public void load() throws IOException {
		if (getFile().exists()) {
			assertFileIsReadable();
			bindings.clear();
			try (FileInputStream fis = new FileInputStream(getFile())) {
				StreamSupport.stream(yaml.loadAll(fis).spliterator(), false)
						.filter(it -> it instanceof Map)
						.map(it -> (Map<?, ?>) it)
						.forEach(untypedMap -> {
							for (Map.Entry<?, ?> untypedEntry : untypedMap.entrySet()) {
								bindings.put(Objects.toString(untypedEntry.getKey()), Objects.toString(untypedEntry.getValue()));
							}
						});
			}
		}
	}

	@Override
	public void save() throws IOException {
		assertFileIsWritable();
		try (OutputStreamWriter writer =
					 new OutputStreamWriter(
							 new FileOutputStream(getFile()), Charset.forName("UTF-8"))) {
			writer.write(yaml.dumpAsMap(bindings));
		}
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
