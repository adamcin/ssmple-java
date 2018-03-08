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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.DeleteParametersRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersByPathRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersByPathResult;
import com.amazonaws.services.simplesystemsmanagement.model.Parameter;
import com.amazonaws.services.simplesystemsmanagement.model.ParameterType;
import com.amazonaws.services.simplesystemsmanagement.model.PutParameterRequest;

/**
 * Selects properties from SSM and syncs values to .properties files on the filesystem.
 */
class Main {
	/**
	 * Turns out this can be between 1 and 10. I'm so glad I made it a parameter.
	 */
	static final int DEFAULT_MAX_RESULTS = 10;

	/**
	 * I think the current directory is the right default here.
	 */
	static final String DEFAULT_CONF_DIR = ".";

	/**
	 * Build an SSM parameter path or name.
	 *
	 * @param pathPrefix hierarchy levels 0-(N-2)
	 * @param filename   hierarchy level N-1 (.properties, .json, or .yaml extensions will be stripped)
	 * @param key        optional, hierarchy level N
	 * @return /-delimited parameter path.
	 */
	static String buildParameterPath(final String pathPrefix, final String filename, final String key) {
		StringBuilder sb = new StringBuilder();
		if (pathPrefix != null) {
			sb.append(pathPrefix);
		}
		if (!sb.toString().endsWith("/")) {
			sb.append("/");
		}
		if (filename == null) {
			sb.append("$");
		} else if (filename.contains(".")) {
			sb.append(filename.substring(0, filename.lastIndexOf(".")));
		} else {
			sb.append(filename);
		}
		if (key != null && !key.isEmpty()) {
			if (!sb.toString().endsWith("/")) {
				sb.append("/");
			}
			sb.append(key);
		}
		return sb.toString();
	}

	/**
	 * Operation to be performed.
	 */
	private enum SsmCmd {
		GET, PUT, DELETE, CLEAR
	}

	private final AWSSimpleSystemsManagementClientBuilder ssmBuilder;

	private AWSSimpleSystemsManagement ssm;

	private final List<String> paramPathPrefixes = new ArrayList<>();

	private final List<String> filenames = new ArrayList<>();

	private File confDir;

	private int fetchSize = DEFAULT_MAX_RESULTS;

	private SsmCmd ssmCmd = SsmCmd.GET;

	private boolean overwritePut;

	private String keyId;

	private Map<String, FileStore> fileStores = new LinkedHashMap<>();

	/**
	 * Pass in the ssmBuilder so it can be modified by CLI params.
	 *
	 * @param ssmBuilder the SSM client builder
	 */
	Main(final AWSSimpleSystemsManagementClientBuilder ssmBuilder) {
		this.ssmBuilder = ssmBuilder;
		this.confDir = new File(DEFAULT_CONF_DIR);
	}

	/**
	 * Get the value.
	 *
	 * @return the value
	 */
	AWSSimpleSystemsManagementClientBuilder getSsmBuilder() {
		return ssmBuilder;
	}

	/**
	 * Get the list for modification.
	 *
	 * @return the paramPathPrefixes list
	 */
	List<String> getParamPathPrefixes() {
		return paramPathPrefixes;
	}

	/**
	 * Get the list for modification.
	 *
	 * @return the filenames list.
	 */
	List<String> getFilenames() {
		return filenames;
	}

	/**
	 * Get the value.
	 *
	 * @return the value
	 */
	File getConfDir() {
		return confDir;
	}

	/**
	 * Set the value.
	 *
	 * @param confDir the value
	 */
	void setConfDir(final File confDir) {
		this.confDir = confDir;
	}

	/**
	 * Get the value.
	 *
	 * @return the value
	 */
	int getFetchSize() {
		return fetchSize;
	}

	/**
	 * Set the value.
	 *
	 * @param fetchSize the value
	 */
	void setFetchSize(final int fetchSize) {
		this.fetchSize = fetchSize;
	}

	/**
	 * Get the value.
	 *
	 * @return the value
	 */
	SsmCmd getSsmCmd() {
		return ssmCmd;
	}

	/**
	 * Set the value.
	 *
	 * @param ssmCmd the value
	 */
	void setSsmCmd(final SsmCmd ssmCmd) {
		this.ssmCmd = ssmCmd;
	}

	/**
	 * Get the value.
	 *
	 * @return the value
	 */
	boolean isOverwritePut() {
		return overwritePut;
	}

	/**
	 * Set the value.
	 *
	 * @param overwritePut the value
	 */
	void setOverwritePut(final boolean overwritePut) {
		this.overwritePut = overwritePut;
	}

	/**
	 * Get the value. Assume Parameter type is SecureString if not null.
	 *
	 * @return the value
	 */
	String getKeyId() {
		return keyId;
	}

	/**
	 * Set the value.
	 *
	 * @param keyId the value
	 */
	void setKeyId(final String keyId) {
		this.keyId = keyId;
	}

	/**
	 * Build a client and go with provided parameters.
	 *
	 * @throws IOException if I/O fails exceptionally
	 */
	void doMain() throws IOException {
		this.ssm = this.ssmBuilder.build();
		if (confDir.exists() && confDir.isDirectory()) {
			for (String filename : this.filenames) {
				FileStore fileStore = AbstractFileStore.getStore(confDir, filename);
				fileStore.load();
				this.fileStores.put(filename, fileStore);
			}

			switch (getSsmCmd()) {
			case GET:
				doGet();
				break;
			case PUT:
				doPut();
				break;
			case DELETE:
				doDelete();
				break;
			case CLEAR:
				doClear();
				break;
			}

		} else {
			throw new IOException("Failed to read conf directory " + confDir.getAbsolutePath());
		}
	}

	// -----------
	// GET methods
	// -----------

	private void doGet() throws IOException {
		if (confDir.mkdirs() || !confDir.canWrite()) {
			throw new IOException("Insufficient permissions to manage conf directory " + confDir.getAbsolutePath());
		}

		for (String filename : filenames) {
			getParamsPerFile(filename, fileStores.get(filename));
		}
	}

	private void getParamsPerFile(final String filename, final FileStore fileStore) throws IOException {
		for (String prefix : paramPathPrefixes) {
			final String parameterPath = buildParameterPath(prefix, filename, null);
			getParamsForPath(parameterPath, fileStore);
		}

		if (!fileStore.getKeys().isEmpty()) {
			fileStore.save();
		}
	}

	private void getParamsForPath(final String parameterPath, final FileStore fileStore) {
		findAllParametersForPath(parameterPath).values().stream()
				.filter(it -> it.getName().startsWith(parameterPath + "/"))
				.forEach(it -> fileStore.putParam(it.getName().substring(parameterPath.length() + 1), it.getValue()));
	}

	// -----------
	// PUT methods
	// -----------

	private void doPut() throws IOException {
		if (this.getParamPathPrefixes().size() != 1) {
			throw new IllegalArgumentException("put command requires exactly one -s/--starts-with argument.");
		}

		for (String filename : filenames) {
			putParamsPerFile(filename, fileStores.get(filename));
		}
	}

	private void putParamsPerFile(final String filename, final FileStore store) throws IOException {
		for (String key : store.getKeys()) {
			final String name = buildParameterPath(this.paramPathPrefixes.get(0), filename, key);
			store.getValue(key).ifPresent(value -> {
				PutParameterRequest req = new PutParameterRequest()
						.withName(name)
						.withValue(value)
						.withOverwrite(isOverwritePut());

				if (getKeyId() != null) {
					req = req
							.withType(ParameterType.SecureString)
							.withKeyId(getKeyId());
				} else {
					req = req.withType(ParameterType.String);
				}

				ssm.putParameter(req);
			});
		}
	}

	// --------------
	// DELETE methods
	// --------------

	private void doDelete() {
		if (this.getParamPathPrefixes().size() != 1) {
			throw new IllegalArgumentException("delete command requires exactly one -s/--starts-with argument.");
		}

		for (String filename : filenames) {
			deleteParamsPerFile(filename, fileStores.get(filename));
		}
	}

	private void deleteParamsPerFile(final String filename, final FileStore fileStore) {
		final String singlePrefix = this.getParamPathPrefixes().get(0);
		final String parameterPath = buildParameterPath(singlePrefix, filename, null);

		Set<String> names = fileStore.getKeys().stream()
				.map(key -> buildParameterPath(singlePrefix, filename, key))
				.collect(Collectors.toSet());

		List<String> parameterNames = findAllParametersForPath(parameterPath).values().stream()
				.map(Parameter::getName)
				.filter(names::contains)
				.collect(Collectors.toList());

		ssm.deleteParameters(new DeleteParametersRequest().withNames(parameterNames));
	}

	// --------------
	// CLEAR methods
	// --------------

	private void doClear() {
		if (this.getParamPathPrefixes().size() != 1) {
			throw new IllegalArgumentException("clear command requires exactly one -s/--starts-with argument.");
		}

		for (String filename : filenames) {
			clearParamsPerFile(filename);
		}
	}

	private void clearParamsPerFile(final String filename) {
		final String parameterPath = buildParameterPath(this.getParamPathPrefixes().get(0), filename, null);

		List<String> parameterNames = findAllParametersForPath(parameterPath).values().stream()
				.map(Parameter::getName)
				.filter(it -> it.startsWith(parameterPath + "/"))
				.collect(Collectors.toList());

		ssm.deleteParameters(new DeleteParametersRequest().withNames(parameterNames));
	}

	// --------------
	// common methods
	// --------------

	private Map<String, Parameter> findAllParametersForPath(final String parameterPath) {
		return findAllParametersForPath(new HashMap<>(), parameterPath, null);
	}

	private Map<String, Parameter> findAllParametersForPath(final Map<String, Parameter> accumulator, final String parameterPath,
			final String nextToken) {
		GetParametersByPathRequest req = new GetParametersByPathRequest()
				.withMaxResults(fetchSize)
				.withPath(parameterPath)
				.withWithDecryption(true)
				.withNextToken(nextToken)
				.withRecursive(false);
		GetParametersByPathResult result = this.ssm.getParametersByPath(req);
		List<Parameter> resultParameters = result.getParameters();
		boolean isLast = resultParameters.isEmpty() || resultParameters.size() < fetchSize;
		String fetchToken = result.getNextToken();
		for (Parameter parameter : resultParameters) {
			accumulator.put(parameter.getName(), parameter);
		}
		if (isLast) {
			return accumulator;
		} else {
			return findAllParametersForPath(accumulator, parameterPath, fetchToken);
		}
	}

	/**
	 * CLI entry point.
	 *
	 * @param args the standard array of command line arguments.
	 * @throws IOException when something breaks
	 */
	public static void main(String[] args) throws IOException {
		Main spp = new Main(AWSSimpleSystemsManagementClientBuilder.standard());

		List<String> argList = Arrays.asList(args);
		Iterator<String> opts = argList.iterator();
		while (opts.hasNext()) {
			String opt = opts.next();
			switch (opt) {
			case "-p":
			case "--profile":
				System.setProperty("aws.profile", opts.next());
				spp.getSsmBuilder().setCredentials(new DefaultAWSCredentialsProviderChain());
				break;
			case "-r":
			case "--region":
				spp.getSsmBuilder().setRegion(opts.next());
				break;
			case "-b":
			case "--batch-size":
				String maxResultsString = opts.next();
				try {
					spp.setFetchSize(Integer.valueOf(maxResultsString));
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException(String.format("Invalid integer for %s: %s", opt, maxResultsString));
				}
				break;
			case "-C":
			case "--conf-dir":
				spp.setConfDir(new File(opts.next()));
				break;
			case "-f":
			case "--filename":
				spp.getFilenames().add(opts.next());
				break;
			case "-s":
			case "--starts-with":
				spp.getParamPathPrefixes().add(opts.next());
				break;
			case "-k":
			case "--key-id":
				spp.setKeyId(opts.next());
				break;
			case "-o":
			case "--overwrite-put":
				spp.setOverwritePut(true);
				break;
			case "put":
				spp.setSsmCmd(SsmCmd.PUT);
				break;
			case "get":
				spp.setSsmCmd(SsmCmd.GET);
				break;
			case "delete":
				spp.setSsmCmd(SsmCmd.DELETE);
				break;
			case "clear":
				spp.setSsmCmd(SsmCmd.CLEAR);
				break;
			default:
				throw new IllegalArgumentException(String.format("Unrecognized option %s.", opt));
			}
		}

		if (spp.getParamPathPrefixes().isEmpty()) {
			throw new IllegalArgumentException("At least one -s/--starts-with path is required, like /ecs/dev/myapp");
		}

		if (spp.getFilenames().isEmpty()) {
			throw new IllegalArgumentException("At least one -f/--filename argument is required, like instance.properties");
		}

		spp.doMain();
	}
}
