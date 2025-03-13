package com.aerospike.migration.importer;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.UnrecognizedOptionException;

import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.RecordExistsAction;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

public class AerospikeImporterOptions {
    private ClusterConfig cluster = null;
    private boolean silent = false;
    private int threads = 1;
    private String mappingFileName;
    private String errorFileName;
    private String inputFileName;
    private String clusterName;
    private String host;
    private String userName;
    private String password;
    private TlsOptions tlsOptions;
    private AuthMode authMode;
    private boolean servicesAlternate;
    private MappingSpecs mappingSpecs;

    private long missingRecordsLimit;
    private RecordExistsAction recordExistsAction;
    private boolean sendKey;
    
    private boolean verbose = false;
    private boolean debug = false;
    private boolean ignoreMissing = false;
    
    private int maxQueueDepth;
    
    static class ParseException extends RuntimeException {
        private static final long serialVersionUID = 5652947902453765251L;

        public ParseException(String message) {
            super(message);
        }
    }
    
    private SSLOptions parseTlsContext(String tlsContext) {
        SSLOptions options = new SSLOptions();
        
        StringWithOffset stringData = new StringWithOffset(tlsContext);
        stringData.checkAndConsumeSymbol('{');
        while (!stringData.isSymbol('}', false)) {
            String subkey = stringData.getString();
            stringData.checkAndConsumeSymbol(':');
            String subValue = stringData.getString();
            if (!stringData.isSymbol('}', false)) {
                stringData.checkAndConsumeSymbol(',');
            }
            switch (subkey) {
            case "certChain":
                options.setCertChain(subValue);
                break;
            case "privateKey":
                options.setPrivateKey(subValue);
                break;
            case "caCertChain":
                options.setCaCertChain(subValue);
                break;
            case "keyPassword":
                options.setKeyPassword(subValue);
                break;
            default: 
                throw new ParseException("Unexpected key '" + subkey + "' in TLS Context. Valid keys are: 'certChain', 'privateKey', 'caCertChain', and 'keyPassword'");
            }
        }
        return options;
    }
    
    private void setPropertyOnTlsPolicy(TlsOptions tlsOptions, String key, String value) {
        switch (key) {
        case "protocols":
            tlsOptions.setProtocols(value);
            break;
        case "ciphers":
            tlsOptions.setCiphers(value);
            break;
        case "revokeCerts":
            tlsOptions.setRevokeCertificates(value);
            break;
        case "loginOnly":
            tlsOptions.setLoginOnly(Boolean.parseBoolean(value));
            break;
        case "context":
            tlsOptions.setSsl(parseTlsContext(value));
            break;
        default: 
            throw new ParseException("Unexpected key '" + key + "' in TLS policy. Valid keys are: 'protocols', 'ciphers', 'revokeCerts', 'context' and 'loginOnly'");
        }
    }
    
    private TlsOptions parseTlsOptions(String tlsOptions) {
        if (tlsOptions != null) {
            TlsOptions policy = new TlsOptions();
            StringWithOffset stringData = new StringWithOffset(tlsOptions);
            if (stringData.isSymbol('{')) {
                while (true) {
                    String key = stringData.getString();
                    if (key != null) {
                        stringData.checkAndConsumeSymbol(':');
                        String value = stringData.getString();
                        setPropertyOnTlsPolicy(policy, key, value);
                    }
                    if (stringData.isSymbol('}')) {
                        break;
                    }
                    else {
                        stringData.checkAndConsumeSymbol(',');
                    }
                }
                
            }
            return policy;
        }
        return null;
    }
    
    private Options formOptions() {
        Options options = new Options();
        options.addOption("m", "mappingFile", true, "YAML file with mappings in it. Every string key in Redis must be mapped to a (namespace, set, id) tuple in Aerospike. This file specifies "
                + "these mappings using regular expressions. This file is required");
        options.addOption("t", "threads", true, "Number of threads to use. Use 0 to use 1 thread per core. (Default: 0)");
        options.addOption("i", "inputFile", true, "Path to a RDB file to import. ");
        options.addOption("ef", "errorFile", true, "Name of file to write errors to, in addtion to stdout");
        options.addOption("rea", "recordExistsAction", true, "Action to take if the record already exists in Aerospike. Values include:\n"
                + "* UPDATE (default) - records are upserted, merging in with existing records.\n"
                + "* REPLACE - record contents become the values of the last update from Redis"
                + "* CREATE_ONLY - only insert records, never overwrite or merge with existing records");
        options.addOption("sk","sendKey", true, "Whether to send the key to the server on each request. Defaults to true");
        options.addOption("q", "quiet", false, "Do not output spurious information like progress.");
        options.addOption("u", "usage", false, "Display the usage and exit.");
        options.addOption("h", "host", true, 
                "List of seed hosts for first cluster in format: " +
                        "hostname1[:tlsname][:port1],...\n" +
                        "The tlsname is only used when connecting with a secure TLS enabled server. " +
                        "If the port is not specified, the default port is used. " +
                        "IPv6 addresses must be enclosed in square brackets.\n" +
                        "Default: localhost\n" +
                        "Examples:\n" +
                        "host1\n" +
                        "host1:3000,host2:3000\n" +
                        "192.168.1.10:cert1:3000,[2001::1111]:cert2:3000\n");
        options.addOption("U", "user", true, "User name for cluster");
        options.addOption("P", "password", true, "Password for cluster");
        options.addOption("ts", "tls", true, "Set the TLS Policy options for the Aerospike cluster. The value passed should be a JSON string. Valid keys in this "
                + "string inlcude 'protocols', 'ciphers', 'revokeCerts', 'context' and 'loginOnly'. For 'context', the value should be a JSON string which "
                + "can contain keys 'certChain' (path to the certificate chain PEM), 'privateKey' (path to the certificate private key PEM), "
                + "'caCertChain' (path to the CA certificate PEM), 'keyPassword' (password used for the certificate chain PEM), 'tlsHost' (the tlsName of the Aerospike host). "
                + "For example: --tls '{\"context\":{\"certChain\":\"cert.pem\",\"privateKey\":\"key.pem\",\"caCertChain\":\"cacert.pem\",\"tlsHost\":\"tls1\"}}'");
        options.addOption("a", "authMode", true, "Set the auth mode of Aerospike cluster. Default: INTERNAL");
        options.addOption("cn", "clusterName", true, "Set the cluster name of the Aerospike cluster");
        options.addOption("sa", "useServicesAlternate", false, "Use services alternative when connecting to the Aerospike cluster");
        options.addOption("V", "verbose", false, "Turn on verbose logging, especially for cluster details and TLS connections");
        options.addOption("D", "debug", false, "Turn on debug mode. This will output a lot of information and automatically turn on verbose mode and turn silent mode off");
        options.addOption("qd", "queueDepth", true, "Specify the maximum queue depth to process from file. (Default: 5000)");
        options.addOption("im", "ignoreMissing", false, "If a record in Redis has a key which does not match any of the mapping specs, silently ignore this record instead of flagging an error.");
        return options;
    }

    private void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        String syntax = AerospikeImporter.class.getName() + " [<options>]";
        formatter.printHelp(pw, 140, syntax, "options:", options, 0, 2, null);
        System.out.println(sw.toString());
        System.exit(1);
    }

    private boolean validateMappingFile() {
        try {
            ObjectMapper mapper = YAMLMapper.builder().enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS).build();
            mapper.findAndRegisterModules();
            mappingSpecs = mapper.readValue(new File(this.mappingFileName), MappingSpecs.class);
            mappingSpecs.validate();
            return true;
        }
        catch (Exception e) {
            System.out.printf("Configuration error: " + e.getMessage());
            return false;
        }
    }
    
    private boolean isValidFile(String name) {
        File file = new File(name);
        return file.isFile() && file.canRead();
    }
    
    private void validate(Options options, CommandLine cl) {
        boolean valid = false;
        
        ClusterConfig cluster = new ClusterConfig();
        cluster.setAuthMode(getAuthMode());
        cluster.setClusterName(getClusterName());
        cluster.setHostName(getHost());
        cluster.setPassword(getPassword());
        cluster.setUserName(getUserName());
        cluster.setUseServicesAlternate(isServicesAlternate());
        cluster.setTls(getTlsOptions());
        if (cluster.getHostName() != null && !cluster.getHostName().isEmpty()) {
            this.cluster = cluster;
            if (this.threads < 0) {
                System.out.println("threads must be >= 0, not " + this.threads);
            }
            else if (this.mappingFileName == null ) {
                System.out.println("Mapping file must be provided");
            }
            else if (!isValidFile(this.inputFileName)) {
                System.out.println("Input file (*.rdb) must exist and be readable");
            }
            else {
                valid = validateMappingFile();
            }
        }
        else {
            System.out.println("Aerospike cluster details must be specified");
        }
        if (!valid) {
            usage(options);
        }
    }
    
    public AerospikeImporterOptions(String[] arguments) throws Exception {
        Options options = formOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine cl = null;
        try {
            cl = parser.parse(options, arguments, false);
        }
        catch (UnrecognizedOptionException uoe) {
            System.out.println("Unrecognized option: " + uoe.getOption());
            usage(options);
        }
        
        if (cl.hasOption("usage")) {
            usage(options);
        }

        this.silent = cl.hasOption("quiet");
        this.threads = Integer.valueOf(cl.getOptionValue("threads", "0"));
        this.inputFileName = cl.getOptionValue("inputFile");
        this.mappingFileName = cl.getOptionValue("mappingFile");
        this.errorFileName = cl.getOptionValue("errorFile");
        this.host = cl.getOptionValue("host");
        this.userName = cl.getOptionValue("user");
        this.password = cl.getOptionValue("password");
        this.tlsOptions = parseTlsOptions(cl.getOptionValue("tls"));
        this.authMode = AuthMode.valueOf(cl.getOptionValue("authMode", "INTERNAL").toUpperCase());
        this.clusterName = cl.getOptionValue("clusterName");
        this.servicesAlternate = cl.hasOption("useServicesAlternate");

        this.recordExistsAction = RecordExistsAction.valueOf(cl.getOptionValue("recordExistsAction", "UPDATE"));
        this.sendKey = Boolean.valueOf(cl.getOptionValue("sendKey", "true"));
        this.ignoreMissing = cl.hasOption("ignoreMissing");
        this.verbose = cl.hasOption("verbose");
        this.debug = cl.hasOption("debug");
        if (this.debug) {
            this.silent = false;
            this.verbose = true;
        }
        this.maxQueueDepth = Integer.valueOf(cl.getOptionValue("queueDepth", "5000"));
        this.validate(options, cl);
    }

    public boolean isSilent() {
        return silent;
    }

    public int getThreads() {
        return threads;
    }

    public ClusterConfig getCluster() {
        return cluster;
    }

    public String getMappingFileName() {
        return mappingFileName;
    }

    public String getErrorFileName() {
        return errorFileName;
    }

    public String getInputFileName() {
        return inputFileName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getHost() {
        return host;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public TlsOptions getTlsOptions() {
        return tlsOptions;
    }

    public AuthMode getAuthMode() {
        return authMode;
    }

    public boolean isServicesAlternate() {
        return servicesAlternate;
    }

    public MappingSpecs getMappingSpecs() {
        return mappingSpecs;
    }

    public long getMissingRecordsLimit() {
        return missingRecordsLimit;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public boolean isDebug() {
        return debug;
    }

    public RecordExistsAction getRecordExistsAction() {
        return recordExistsAction;
    }

    public boolean isSendKey() {
        return sendKey;
    }

    public int getMaxQueueDepth() {
        return maxQueueDepth;
    }
    
    public boolean isIgnoreMissing() {
        return ignoreMissing;
    }
}
