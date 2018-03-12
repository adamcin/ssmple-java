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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.amazonaws.services.kms.model.AliasListEntry;
import com.amazonaws.services.kms.model.ListAliasesResult;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.DeleteParametersRequest;
import com.amazonaws.services.simplesystemsmanagement.model.DescribeParametersRequest;
import com.amazonaws.services.simplesystemsmanagement.model.DescribeParametersResult;
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersByPathRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersByPathResult;
import com.amazonaws.services.simplesystemsmanagement.model.Parameter;
import com.amazonaws.services.simplesystemsmanagement.model.ParameterType;
import com.amazonaws.services.simplesystemsmanagement.model.ParametersFilter;
import com.amazonaws.services.simplesystemsmanagement.model.ParametersFilterKey;
import com.amazonaws.services.simplesystemsmanagement.model.PutParameterRequest;

/**
 * Selects properties from SSM and syncs values to .properties files on the filesystem.
 */
class Main {
	/**
	 * Property Key / Param Name suffix used for serializing KMS key IDs alongside SecureString values.
	 */
	static final String KEY_ID_SUFFIX = "_SecureStringKeyId";

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

	private String keyIdPut;

	private boolean noStoreSecureString;

	private boolean noPutSecureString;

	private boolean clearOnPut;

	private Map<String, FileStore> fileStores = new LinkedHashMap<>();

	private final AWSKMSClientBuilder kmsBuilder;

	private AWSKMS kms;

	private final Map<String, String> aliasesToKeys = new HashMap<>();

	private final Map<String, String> keysToAliases = new HashMap<>();

	/**
	 * Pass in the ssmBuilder so it can be modified by CLI params.
	 *
	 * @param ssmBuilder the SSM client builder
	 */
	Main(final AWSSimpleSystemsManagementClientBuilder ssmBuilder, final AWSKMSClientBuilder kmsBuilder) {
		this.ssmBuilder = ssmBuilder;
		this.confDir = new File(DEFAULT_CONF_DIR);
		this.kmsBuilder = kmsBuilder;
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
	String getKeyIdPut() {
		return keyIdPut;
	}

	/**
	 * Set the value.
	 *
	 * @param keyIdPut the value
	 */
	void setKeyIdPutAll(final String keyIdPut) {
		this.keyIdPut = keyIdPut;
	}

	/**
	 * Get the value.
	 *
	 * @return the value
	 */
	public boolean isNoStoreSecureString() {
		return noStoreSecureString;
	}

	/**
	 * Set the value.
	 *
	 * @param noStoreSecureString the value
	 */
	public void setNoStoreSecureString(final boolean noStoreSecureString) {
		this.noStoreSecureString = noStoreSecureString;
	}

	/**
	 * Get the value.
	 *
	 * @return the value
	 */
	public boolean isNoPutSecureString() {
		return noPutSecureString;
	}

	/**
	 * Set the value.
	 *
	 * @param noPutSecureString the value
	 */
	public void setNoPutSecureString(final boolean noPutSecureString) {
		this.noPutSecureString = noPutSecureString;
	}

	/**
	 * Get the value.
	 *
	 * @return the value
	 */
	public boolean isClearOnPut() {
		return clearOnPut;
	}

	/**
	 * Set the value.
	 *
	 * @param noClearOnPut the value
	 */
	public void setClearOnPut(final boolean noClearOnPut) {
		this.clearOnPut = noClearOnPut;
	}

	/**
	 * Get the value.
	 *
	 * @return the value
	 */
	public AWSKMSClientBuilder getKmsBuilder() {
		return kmsBuilder;
	}

	private void buildAliasList() {
		if (this.kms == null) {
			this.kms = this.kmsBuilder.build();
		}

		final ListAliasesResult result = this.kms.listAliases();
		for (AliasListEntry entry : result.getAliases()) {
			if (entry.getTargetKeyId() != null && !entry.getTargetKeyId().isEmpty()) {
				this.aliasesToKeys.put(entry.getAliasName(), entry.getTargetKeyId());
				this.keysToAliases.put(entry.getTargetKeyId(), entry.getAliasName());
			}
		}
	}

	String derefAlias(final String alias) {
		final String fqAlias;
		if (alias.startsWith("alias/")) {
			fqAlias = alias;
		} else {
			fqAlias = "alias/" + alias;
		}
		return this.aliasesToKeys.getOrDefault(fqAlias, fqAlias);
	}

	String getAliasForKeyId(String keyId) {
		return this.keysToAliases.getOrDefault(keyId, keyId);
	}

	static String getCanonicalPath(File file) {
		try {
			return file.getCanonicalPath();
		} catch (IOException e) {
			e.printStackTrace(System.err);
		}
		return file.getAbsolutePath();
	}

	Iterable<String> getResolvedFilenames() {
		final File basedir = getConfDir();
		final String basepath = getCanonicalPath(basedir) + "/";
		return this.filenames.stream()
				.map(it -> getCanonicalPath(new File(basedir, it)))
				.filter(it -> it.startsWith(basepath))
				.map(it -> it.substring(basepath.length()))
				.collect(Collectors.toList());
	}

	/**
	 * Build a client and go with provided parameters.
	 *
	 * @throws IOException if I/O fails exceptionally
	 */
	void doMain() throws IOException {
		this.ssm = this.ssmBuilder.build();
		if (getConfDir().exists() && getConfDir().isDirectory()) {

			for (String filename : getResolvedFilenames()) {
				FileStore fileStore = AbstractFileStore.getStore(getConfDir(), filename);
				fileStore.load();
				this.fileStores.put(filename, fileStore);
			}

			switch (getSsmCmd()) {
			case GET:
				if (!isNoStoreSecureString()) {
					this.buildAliasList();
				}
				doGet();
				break;
			case PUT:
				if (!isNoPutSecureString()) {
					this.buildAliasList();
				}
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
			throw new IOException("Failed to read conf directory " + getConfDir().getAbsolutePath());
		}
	}

	// -----------
	// GET methods
	// -----------

	private void doGet() throws IOException {
		if (getConfDir().mkdirs() || !getConfDir().canWrite()) {
			throw new IOException("Insufficient permissions to manage conf directory " + getConfDir().getAbsolutePath());
		}

		for (String filename : getResolvedFilenames()) {
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

	/**
	 * If value is all spaces, subtract a space to reconstruct the original value for export.
	 *
	 * @param value parameter value.
	 * @return unescaped value
	 */
	private static String unescapeValueAfterGet(final String value) {
		if (value.isEmpty()) {
			return value;
		}

		for (int i = 0; i < value.length(); i++) {
			if (value.charAt(i) != ' ') {
				return value;
			}
		}

		return value.substring(0, value.length() - 1);
	}

	/**
	 * If value is the empty string or all spaces, add a space so the value is non-empty for SSM.
	 *
	 * @param value parameter value.
	 * @return escaped value
	 */
	private static String escapeValueBeforePut(final String value) {
		for (int i = 0; i < value.length(); i++) {
			if (value.charAt(i) != ' ') {
				return value;
			}
		}

		return value + " ";
	}

	private void getParamsForPath(final String parameterPath, final FileStore fileStore) {
		findAllParametersForPath(parameterPath).values().stream()
				.filter(it -> it.getName().startsWith(parameterPath + "/"))
				.forEach(it -> {
					final String storeKey = it.getName().substring(parameterPath.length() + 1);
					if (ParameterType.fromValue(it.getType()) != ParameterType.SecureString || !isNoStoreSecureString()) {
						fileStore.putParam(storeKey, unescapeValueAfterGet(it.getValue()));

						if (ParameterType.fromValue(it.getType()) == ParameterType.SecureString) {
							final String sidecarStoreKey = storeKey + KEY_ID_SUFFIX;
							DescribeParametersResult result = ssm.describeParameters(
									new DescribeParametersRequest()
											.withFilters(
													new ParametersFilter().withKey(ParametersFilterKey.Name).withValues(it.getName())));

							Optional<String> paramKeyId = result.getParameters().stream()
									.findFirst()
									.flatMap(meta -> Optional.ofNullable(meta.getKeyId()))
									.map(this::getAliasForKeyId);

							paramKeyId.ifPresent(keyValue -> fileStore.putParam(sidecarStoreKey, keyValue));
						}
					}
				});
	}

	// -----------
	// PUT methods
	// -----------

	private void doPut() {
		if (this.getParamPathPrefixes().size() != 1) {
			throw new IllegalArgumentException("put command requires exactly one -s/--starts-with argument.");
		}

		for (String filename : getResolvedFilenames()) {
			putParamsPerFile(filename, this.paramPathPrefixes.get(0), fileStores.get(filename));
		}
	}

	private void putParamsPerFile(final String filename, final String paramPathPrefix, final FileStore store) {
		Set<String> storeKeys = store.getKeys();
		if (isClearOnPut()) {
			clearParamsPerFile(filename, paramPathPrefix);
		}
		for (String key : storeKeys.stream().filter(it -> !it.endsWith(KEY_ID_SUFFIX)).collect(Collectors.toSet())) {
			final String sidecarKeyId = key + KEY_ID_SUFFIX;
			final String name = buildParameterPath(this.paramPathPrefixes.get(0), filename, key);

			if (isNoPutSecureString() && storeKeys.contains(sidecarKeyId)) {
				continue;
			}

			store.getValue(key).ifPresent(value -> {
				final PutParameterRequest req = new PutParameterRequest()
						.withName(name)
						.withType(ParameterType.String)
						.withValue(escapeValueBeforePut(value))
						.withOverwrite(isOverwritePut());

				Optional<String> paramKeyId = Stream.of(Optional.ofNullable(getKeyIdPut()), store.getValue(sidecarKeyId))
						.filter(Optional::isPresent)
						.map(Optional::get)
						.map(this::derefAlias)
						.findFirst();

				ssm.putParameter(paramKeyId.map(keyValue -> req.withType(ParameterType.SecureString).withKeyId(keyValue)).orElse(req));
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

		for (String filename : getResolvedFilenames()) {
			deleteParamsPerFile(filename, fileStores.get(filename));
		}
	}

	private void deleteParamsPerFile(final String filename, final FileStore fileStore) {
		final String singlePrefix = this.getParamPathPrefixes().get(0);
		final String parameterPath = buildParameterPath(singlePrefix, filename, null);

		Set<String> names = fileStore.getKeys().stream()
				.map(key -> buildParameterPath(singlePrefix, filename, key))
				.collect(Collectors.toSet());

		List<Parameter> parameters = new ArrayList<>(findAllParametersForPath(parameterPath).values());

		ofSubLists(parameters, 10)
				.map(toDelete -> toDelete.stream().map(Parameter::getName).filter(names::contains).collect(Collectors.toList()))
				.filter(toDelete -> toDelete.size() > 0)
				.forEach(toDelete -> ssm.deleteParameters(new DeleteParametersRequest().withNames(toDelete)));
	}

	// --------------
	// CLEAR methods
	// --------------

	private void doClear() {
		if (this.getParamPathPrefixes().size() != 1) {
			throw new IllegalArgumentException("clear command requires exactly one -s/--starts-with argument.");
		}

		for (String filename : getResolvedFilenames()) {
			clearParamsPerFile(filename, this.getParamPathPrefixes().get(0));
		}
	}

	private static boolean isClearableParameter(final String pathPrefix, final String paramPath) {
		return paramPath.startsWith(pathPrefix + "/");
	}

	private void clearParamsPerFile(final String filename, final String paramPathPrefix) {
		final String parameterPath = buildParameterPath(paramPathPrefix, filename, null);

		List<Parameter> parameters = new ArrayList<>(findAllParametersForPath(parameterPath).values());

		ofSubLists(parameters, 10)
				.map(toDelete -> toDelete.stream().map(Parameter::getName).filter(it -> isClearableParameter(parameterPath, it))
						.collect(Collectors.toList()))
				.filter(toDelete -> toDelete.size() > 0)
				.forEach(toDelete -> ssm.deleteParameters(new DeleteParametersRequest().withNames(toDelete)));
	}

	// --------------
	// common methods
	// --------------

	private static <T> Stream<List<T>> ofSubLists(final List<T> source, final int length) {
		if (length <= 0)
			throw new IllegalArgumentException("length = " + length);
		int size = source.size();
		if (size <= 0)
			return Stream.empty();
		int fullChunks = (size - 1) / length;
		return IntStream.range(0, fullChunks + 1).mapToObj(
				n -> source.subList(n * length, n == fullChunks ? size : (n + 1) * length));
	}

	private Map<String, Parameter> findAllParametersForPath(final String parameterPath) {
		return findAllParametersForPath(new HashMap<>(), parameterPath, null);
	}

	private Map<String, Parameter> findAllParametersForPath(final Map<String, Parameter> accumulator, final String parameterPath,
			final String nextToken) {
		GetParametersByPathRequest req = new GetParametersByPathRequest()
				.withMaxResults(getFetchSize())
				.withPath(parameterPath)
				.withWithDecryption(true)
				.withNextToken(nextToken)
				.withRecursive(false);
		GetParametersByPathResult result = this.ssm.getParametersByPath(req);
		List<Parameter> resultParameters = result.getParameters();
		String fetchToken = result.getNextToken();
		boolean isLast = fetchToken == null || fetchToken.isEmpty() || resultParameters.isEmpty() || resultParameters.size() < getFetchSize();
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
		Main spp = new Main(AWSSimpleSystemsManagementClientBuilder.standard(), AWSKMSClientBuilder.standard());

		List<String> argList = Arrays.asList(args);
		Iterator<String> opts = argList.iterator();
		while (opts.hasNext()) {
			final String opt = opts.next();
			final boolean isNoSwitch = opt.startsWith("--no-");
			switch (isNoSwitch ? opt.replaceFirst("^--no-", "--") : opt) {
			case "-p":
			case "--profile":
				System.setProperty("aws.profile", opts.next());
				spp.getSsmBuilder().setCredentials(new DefaultAWSCredentialsProviderChain());
				spp.getKmsBuilder().setCredentials(new DefaultAWSCredentialsProviderChain());
				break;
			case "-r":
			case "--region":
				String region = opts.next();
				spp.getSsmBuilder().setRegion(region);
				spp.getKmsBuilder().setRegion(region);
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
			case "--key-id-put-all":
				spp.setKeyIdPutAll(opts.next());
				break;
			case "-o":
			case "--overwrite-put":
				spp.setOverwritePut(!isNoSwitch);
				break;
			case "--clear-on-put":
				spp.setClearOnPut(!isNoSwitch);
				break;
			case "--store-secure-string":
				spp.setNoStoreSecureString(isNoSwitch);
				break;
			case "--put-secure-string":
				spp.setNoPutSecureString(isNoSwitch);
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
