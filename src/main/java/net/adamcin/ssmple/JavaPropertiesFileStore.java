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
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

class JavaPropertiesFileStore extends AbstractFileStore {

	private final Properties properties = new Properties();

	JavaPropertiesFileStore(final File file) {
		super(file);
	}

	@Override
	public void load() throws IOException {
		properties.clear();
		if (getFile().exists()) {
			assertFileIsReadable();
			try (FileInputStream fis = new FileInputStream(getFile())) {
				properties.load(fis);
			}
		}
	}

	@Override
	public void save() throws IOException {
		assertFileIsWritable();
		try (FileOutputStream fos = new FileOutputStream(getFile())) {
			properties.store(fos, "Saved from SSM");
		}
	}

	@Override
	public Set<String> getKeys() {
		return properties.stringPropertyNames();
	}

	@Override
	public Optional<String> getValue(final String key) {
		return Optional.ofNullable(properties.getProperty(key));
	}

	@Override
	public void putParam(final String key, final String value) {
		properties.setProperty(key, value);
	}
}
