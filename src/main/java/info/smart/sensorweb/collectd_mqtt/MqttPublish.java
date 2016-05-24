package info.smart.sensorweb.collectd_mqtt;

import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.collectd.api.Collectd;
import org.collectd.api.CollectdConfigInterface;
import org.collectd.api.CollectdInitInterface;
import org.collectd.api.CollectdLogInterface;
import org.collectd.api.CollectdShutdownInterface;
import org.collectd.api.OConfigItem;
import org.collectd.api.OConfigValue;
import org.collectd.api.ValueList;
import org.collectd.api.CollectdWriteInterface;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;

public class MqttPublish implements CollectdWriteInterface, CollectdConfigInterface,
		CollectdInitInterface, CollectdShutdownInterface, CollectdLogInterface {

	String mqttHost = "tcp://localhost:1883";
	String mqttClientId = "collectd-plugin-demo";
	String mqttUserame = "sensor";
	String mqttPassword = "demo";
	String mqttTopicPrefix = "collectd";

	MQTT mqtt;
	BlockingConnection connection;

	public MqttPublish() {

		Collectd.registerInit("MqttPublish", this);
		Collectd.registerConfig("MqttPublish", this);
		Collectd.registerLog("MqttPublish", this);
		Collectd.registerWrite("MqttPublish", this);
		Collectd.registerShutdown("MqttPublish", this);
	}

	@Override
	public int write(ValueList vl) {

		if (connection != null && mqtt != null) {

			try {

				List<Number> values = vl.getValues();
				String sType = vl.getType();
				long lTime = vl.getTime();
				Date dTime = new Date(lTime);
				String sTime = dTime.toString();
				String sTypeInst = vl.getTypeInstance();
				String sPlugin = vl.getPlugin();
				String sPluginInst = vl.getPluginInstance();
				String sHost = vl.getHost();
				long lInterv = vl.getInterval();
				String sInterv = Long.toString(lInterv);

				StringBuilder sbPayload = new StringBuilder();
				sbPayload.append("{ date: '" + sTime + "'; values : { ");

				StringBuilder sbtopic = new StringBuilder();
				sbtopic.append(mqttTopicPrefix + "/" + sHost + "/" + sPlugin);

				if (sPluginInst != null && !sPluginInst.isEmpty()) {
					sbtopic.append("/" + sPluginInst);
				}
				if (sType != null && !sType.isEmpty()) {
					sbtopic.append("/" + sType);
				}
				if (sTypeInst != null && !sTypeInst.isEmpty()) {
					sbtopic.append("/" + sTypeInst);
				}

				int i = 0;
				for (Number value : values) {
					String sVal = value.toString();
					sbPayload.append("'" + sVal + "'");
					i++;
					if ( i < values.size()) {
						sbPayload.append(", ");
					}
				}
				sbPayload.append(" } }");
				byte[] payload = sbPayload.toString().getBytes();

				connection.publish(sbtopic.toString(), payload,
						QoS.AT_MOST_ONCE, false);

			} catch (Exception e) {
				e.printStackTrace();
				Collectd.logError("mqtt publish failed, " + e.getMessage());
				return 2;
			}

			return 0;

		} else {
			return 1;
		}
	}

	@Override
	public void log(int arg0, String arg1) {

		if (connection != null && mqtt != null) {

			try {
				String logmessage = arg1 + " loglevel " + arg0;
				byte[] payload = logmessage.getBytes();

				connection.publish("collectd/sensors/demo", payload,
						QoS.AT_LEAST_ONCE, false);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public int shutdown() {

		try {
			connection.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
			Collectd.logError("mqtt shutdown failed, " + e.getMessage());
		}
		return 0;
	}

	@Override
	public int init() {
		Collectd.logInfo("MQTT Plugin Init");
		try {
			mqtt = new MQTT();
			mqtt.setHost(mqttHost);
			mqtt.setClientId(mqttClientId);
			mqtt.setUserName(mqttUserame);
			mqtt.setPassword(mqttPassword);

			connection = mqtt.blockingConnection();

			connection.connect();

		} catch (Exception e) {

			e.printStackTrace();
			Collectd.logError("mqtt connection could not be established, "
					+ e.getMessage());
			System.exit(-2);
		} finally {
			Collectd.logInfo("mqtt connection established");
		}

		return 0;
	}

	@Override
	public int config(OConfigItem plugin) {
		Collectd.logInfo("MQTT Plugin Configuration - ");

		String pluginkeyhint = plugin.getKey();
		Collectd.logInfo("config plugin key hint: " + pluginkeyhint);

		// plugin one value is its name seemingly
		Iterator<OConfigValue> valiter = plugin.getValues().iterator();
		while (valiter.hasNext()) {
			OConfigValue pluginnameholder = valiter.next();
			if (pluginnameholder.getType() == OConfigValue.OCONFIG_TYPE_STRING ) {
				String pluginname = pluginnameholder.getString();
				Collectd.logInfo("config valiter: " + pluginname);
			}
		}

		int childrenl = plugin.getChildren().size();
		Collectd.logInfo("config configelements count: " + childrenl);

		Iterator<OConfigItem> configelementsiter = plugin.getChildren().iterator();
		while (configelementsiter.hasNext()) {
			OConfigItem actualconfigelement = configelementsiter.next();
			String configelementidentifier = actualconfigelement.getKey();
			Collectd.logInfo("config configelementidentifier: " + configelementidentifier);

			for (OConfigValue subelemvalues : actualconfigelement.getValues()) {
				if (subelemvalues.getType() == OConfigValue.OCONFIG_TYPE_STRING ) {
					Collectd.logInfo("config subelemvalues: " + subelemvalues.getString());

					if (configelementidentifier.equalsIgnoreCase("mqttHost")) {
						mqttHost = subelemvalues.getString();
					} else if (configelementidentifier.equalsIgnoreCase("mqttClientId")) {
						mqttClientId = subelemvalues.getString();
					} else if (configelementidentifier.equalsIgnoreCase("mqttUserame")) {
						mqttUserame = subelemvalues.getString();
					} else if (configelementidentifier.equalsIgnoreCase("mqttPassword")) {
						mqttPassword = subelemvalues.getString();
					} else if (configelementidentifier.equalsIgnoreCase("mqttTopicPrefix")) {
						mqttTopicPrefix = subelemvalues.getString();
					}
				}
			}
		}

		Collectd.logInfo("MQTT Plugin Configuration - done ");
		return 0;
	}

}
