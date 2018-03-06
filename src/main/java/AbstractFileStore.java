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

abstract class AbstractFileStore implements FileStore {

	/**
	 * Identifier for JSON files.
	 */
	static final String EXT_JSON = ".json";

	/**
	 * Identifier for YAML files.
	 */
	static final String EXT_YAML = ".yaml";

	/**
	 * Another identifier for yaml files.
	 */
	static final String EXT_YML = ".yml";

	private final File file;

	AbstractFileStore(final File file) {
		this.file = file;
	}

	@Override
	public File getFile() {
		return file;
	}

	final void assertFileIsReadable() throws IOException {
		if (getFile().exists()) {
			if (getFile().isDirectory() || !getFile().canRead()) {
				throw new IOException("File exists but is not readable: " + getFile().getAbsolutePath());
			}
		}
	}

	final void assertFileIsWritable() throws IOException {
		if (getFile().exists()) {
			if (getFile().isDirectory() || !getFile().canWrite()) {
				throw new IOException("File exists but is not writable: " + getFile().getAbsolutePath());
			}
		} else if (!getFile().createNewFile()) {
			throw new IOException("File could not be created: " + getFile().getAbsolutePath());
		}
	}

	static FileStore getStore(final File confDir, final String filename) throws IOException {
		File localFile = new File(confDir, filename);
		if (filename.endsWith(EXT_JSON)) {
			return new JsonFileStore(localFile);
		} else if (filename.endsWith(EXT_YAML) || filename.endsWith(EXT_YML)) {
			return new YamlFileStore(localFile);
		}
		// serialize as Java properties by default.
		return new JavaPropertiesFileStore(localFile);
	}

}
