/**
 *
 */
package it.geosolutions.utils.db;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.validation.InvalidArgumentException;
import org.apache.commons.cli2.validation.Validator;
import org.geotools.GML;
import org.geotools.GML.Version;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.IllegalAttributeException;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTS;
import org.geotools.jdbc.JDBCDataStoreFactory;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.xml.sax.SAXException;

import com.vividsolutions.jts.geom.Geometry;


/**
 * @authors Fabiani, Ivano Picco
 *
 */
public class Gml2Orcl extends BaseArgumentsManager
{

    private static final int DEFAULT_ORACLE_PORT = 1521;

    private static final String VERSION = "0.3";
    private static final String NAME = "gml2Orcl";

    private static String hostname;
    private static Integer port;
    private static String database;
    private static String schema;
    private static String user;
    private static String password;
    private static String gmlfile;

    private static Map<String, Serializable> orclMap = new HashMap<String, Serializable>();

    /**
     * @param args
     */
    public static void main(String[] args)
    {
        Gml2Orcl gml2orcl = new Gml2Orcl();
        if (!gml2orcl.parseArgs(args))
        {
            System.exit(1);
        }
        try
        {
            initOrclMap();
            gml2orcl.importGml(new File(gmlfile));
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (FactoryException e)
        {
            e.printStackTrace();
        }
        catch (TransformException e)
        {
            e.printStackTrace();
        }
        catch (MismatchedDimensionException e)
        {
            e.printStackTrace();
        }
        catch (IndexOutOfBoundsException e)
        {
            e.printStackTrace();
        }
        catch (IllegalAttributeException e)
        {
            e.printStackTrace();
        }
        catch (SAXException e)
        {
            e.printStackTrace();
        }
        catch (ParserConfigurationException e)
        {
            e.printStackTrace();
        }
    }
    // ////////////////////////////////////////////////////////////////////////
    //
    // ////////////////////////////////////////////////////////////////////////

    private static void initOrclMap()
    {
        orclMap.put(JDBCDataStoreFactory.DBTYPE.key, "Oracle");
        orclMap.put(JDBCDataStoreFactory.HOST.key, hostname);
        orclMap.put(JDBCDataStoreFactory.PORT.key, port);
        orclMap.put(JDBCDataStoreFactory.DATABASE.key, database);
        orclMap.put(JDBCDataStoreFactory.SCHEMA.key, schema);
        orclMap.put(JDBCDataStoreFactory.USER.key, user);
        orclMap.put(JDBCDataStoreFactory.PASSWD.key, password);
        orclMap.put(JDBCDataStoreFactory.MINCONN.key, 1);
        orclMap.put(JDBCDataStoreFactory.MAXCONN.key, 10);
        // orclMap.put(JDBCDataStoreFactory.NAMESPACE.key,
        // "http://www.fao.org/fi");
    }

    /**
     * When loading from DTO use the params to locate factory.
     *
     * <p>
     * bleck
     * </p>
     *
     * @param params
     *
     * @return
     */
    public static DataStoreFactorySpi aquireFactory(Map params)
    {
        for (Iterator i = DataStoreFinder.getAvailableDataStores(); i.hasNext();)
        {
            DataStoreFactorySpi factory = (DataStoreFactorySpi) i.next();

            if (factory.canProcess(params))
            {
                return factory;
            }
        }

        return null;
    }

    private Option hostnameOpt;
    private Option portOpt;
    private Option databaseOpt;
    private Option schemaOpt;
    private Option userOpt;
    private Option passwordOpt;
    private Option gmlfileOpt;


    /**
     * Default constructor
     */
    public Gml2Orcl()
    {
        super(NAME, VERSION);

        // /////////////////////////////////////////////////////////////////////
        // Options for the command line
        // /////////////////////////////////////////////////////////////////////
        gmlfileOpt =
            optionBuilder.withShortName("s").withLongName(
                "gmlfile").withArgument(
                argumentBuilder.withName("filename").withMinimum(1).withMaximum(1).create()).withDescription(
                "gmlfile to import").withRequired(true).create();
        hostnameOpt =
            optionBuilder.withShortName("H").withLongName(
                "hostname").withArgument(
                argumentBuilder.withName("hostname").withMinimum(1).withMaximum(1).create()).withDescription(
                "database host").withRequired(true).create();
        databaseOpt =
            optionBuilder.withShortName("d").withShortName("db").withLongName(
                "database").withArgument(
                argumentBuilder.withName("dbname").withMinimum(1).withMaximum(1).create()).withDescription(
                "database name").withRequired(true).create();
        schemaOpt =
            optionBuilder.withShortName("S").withLongName(
                "schema").withArgument(
                argumentBuilder.withName("schema").withMinimum(1).withMaximum(1).create()).withDescription(
                "database schema").withRequired(true).create();
        userOpt =
            optionBuilder.withShortName("u").withLongName(
                "user").withArgument(
                argumentBuilder.withName("username").withMinimum(1).withMaximum(1).create()).withDescription(
                "username").withRequired(false).create();
        passwordOpt =
            optionBuilder.withShortName("p").withLongName(
                "password").withArgument(
                argumentBuilder.withName("password").withMinimum(1).withMaximum(1).create()).withDescription(
                "password").withRequired(false).create();

        portOpt =
            optionBuilder.withShortName("P").withLongName("port").withDescription("database port").withArgument(
                argumentBuilder.withName("portnumber").withMinimum(1).withMaximum(1).withValidator(
                    new Validator()
                    {

                        public void validate(List args) throws InvalidArgumentException
                        {
                            final int size = args.size();
                            if (size > 1)
                            {
                                throw new InvalidArgumentException(
                                    "Only one port at a time can be defined");
                            }

                            final String val = (String) args.get(0);

                            final int value = Integer.parseInt(val);
                            if ((value <= 0) || (value > 65536))
                            {
                                throw new InvalidArgumentException(
                                    "Invalid port specification");
                            }

                        }
                    }).create()).withRequired(false).create();

        addOption(gmlfileOpt);
        addOption(databaseOpt);
        addOption(hostnameOpt);
        addOption(portOpt);
        addOption(schemaOpt);
        addOption(userOpt);
        addOption(passwordOpt);

        // /////////////////////////////////////////////////////////////////////
        //
        // Help Formatter
        //
        // /////////////////////////////////////////////////////////////////////
        finishInitialization();

    }

    @Override
    public boolean parseArgs(String[] args)
    {
        if (!super.parseArgs(args))
        {
            return false;
        }
        gmlfile = (String) getOptionValue(gmlfileOpt);
        database = (String) getOptionValue(databaseOpt);
        port = hasOption(portOpt) ? Integer.valueOf((String) getOptionValue(portOpt)) : DEFAULT_ORACLE_PORT;
        schema = (String) getOptionValue(schemaOpt);
        user = hasOption(userOpt) ? (String) getOptionValue(userOpt) : null;
        password = hasOption(passwordOpt) ? (String) getOptionValue(passwordOpt) : null;
        hostname = (String) getOptionValue(hostnameOpt);


        return true;
    }

    public void importGml(File gmlFile) throws IOException, IllegalAttributeException, FactoryException,
        MismatchedDimensionException, IndexOutOfBoundsException, TransformException, SAXException,
        ParserConfigurationException
    {

        DataStore orclDataStore = aquireFactory(orclMap).createDataStore(orclMap);
        log("importing gmlfile " + gmlFile);

        long startwork = System.currentTimeMillis();

        InputStream in = gmlFile.toURI().toURL().openStream();

        GML gml = new GML(Version.WFS1_0);
        SimpleFeatureCollection featureCollection = gml.decodeFeatureCollection(in);

        String typeName = featureCollection.getSchema().getTypeName();

        /** Importing GML Data to DB **/
        SimpleFeatureType ftSchema = featureCollection.getSchema();
        
        SimpleFeatureType targetSchema;
		// build the schema type
		try {
			SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
			tb.setName(typeName);
			
			for(AttributeDescriptor att : ftSchema.getAttributeDescriptors())
	        {
	        	if(
        				!att.getLocalName().equals("name") &&
        				!att.getLocalName().equals("description") &&
        				!att.getLocalName().equals("boundedBy")
        		)
	        	{
	        		tb.add(att);
	        	}
	        }
			
			targetSchema = tb.buildFeatureType();
		} catch (Exception e) {
			throw new RuntimeException(
					"Failed to import data into the target store", e);
		}//try:catch
		
        // create the schema for the new shape file
        try
        {
            // FTWrapper pgft = new FTWrapper(shpDataStore.getSchema(ftName));
            // pgft.setReplaceTypeName(tablename);
            orclDataStore.createSchema(targetSchema);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            // Most probably the schema already exists in the DB
            log("Error while creating schema '" + typeName + "': " + e.getMessage());
            log("Will try to load featuretypes bypassing the error.");

            // orclDataStore.updateSchema(typeNames[t],
            // dataStore.getSchema(typeNames[t]));
        }

        // get a feature writer
        FeatureWriter<?, SimpleFeature> fw = orclDataStore.getFeatureWriter(typeName.toUpperCase(), Transaction.AUTO_COMMIT);

        // /////////////////////////////////////////////////////////////////////
        //
        // create the features
        //
        // /////////////////////////////////////////////////////////////////////
        SimpleFeature feature = null;

        final FeatureIterator<?> fr = featureCollection.features();

        final int size = featureCollection.size();

        final CoordinateReferenceSystem sourceCRS =
            featureCollection.getSchema().getGeometryDescriptor().getCoordinateReferenceSystem();
        final CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:4326", true);
        final MathTransform srcCRSToWGS84 = CRS.findMathTransform((sourceCRS != null) ? sourceCRS : targetCRS, targetCRS, true);

        int cnt = 0;
        while (fr.hasNext())
        {
            SimpleFeature srcFeature = (SimpleFeature) fr.next();

            // avoid illegal state
            fw.hasNext();
            feature = fw.next();

            if ((cnt % 50) == 0)
            {
                log("inserting ft #" + cnt + "/" + size + " in " + typeName);
            }

            if (srcFeature != null)
            {
                for (AttributeDescriptor attribute : srcFeature.getFeatureType().getAttributeDescriptors())
                {
                	if(attribute != null)
                		if (
                				!attribute.getLocalName().equals("name") &&
                				!attribute.getLocalName().equals("description") &&
                				!attribute.getLocalName().equals("boundedBy")
                		)
                			if (srcFeature.getAttribute(attribute.getName()) instanceof Geometry)
                			{

                				/** get the original geometry and put it as is into the DB ... **/
                				Geometry defGeom = (Geometry) srcFeature.getAttribute(attribute.getName());

                				/** if we need to reproject the geometry before inserting into the DB ... **/
                				if (!srcCRSToWGS84.isIdentity())
                				{
                					defGeom = JTS.transform((Geometry) attribute, srcCRSToWGS84);
                				}

                				defGeom.setSRID(999999);

                				feature.setAttribute(attribute.getName(), defGeom.buffer(0));
                			}
                			else
                			{
                				feature.setAttribute(attribute.getName(), srcFeature.getAttribute(attribute.getName()));
                			}
                }
                fw.write();
                cnt++;
            }
        }
        fr.close();

        try
        {
            fw.close();
        }
        catch (Exception whatever)
        {
            // amen
        }

        /** Importing SHP Data to DB - END **/
        long endwork = System.currentTimeMillis();

        log(" *** Inserted " + cnt + " features in " + typeName + " in " + (endwork - startwork) + "ms");
    }

    private void log(String msg)
    {
        System.out.println(getClass().getSimpleName() + ": " + msg);
    }
}
