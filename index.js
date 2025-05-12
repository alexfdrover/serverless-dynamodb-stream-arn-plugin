"use strict";

const AWS = require("aws-sdk");

class fetchDynamoDBStreamsPlugin {
	constructor(serverless, options) {
		this.serverless = serverless;
		this.options = options;
		this.configurationVariablesSources = {
			fetchStreamARN: {
				async resolve({ address, params, resolveConfigurationProperty, __ }) {
					let myStreamArn = null;
					try {
						if(!params || params.length < 1)
							throw new Error("No table name passed to fetchStreamARN Function");
						const configuredRegion =
							await resolveConfigurationProperty([
								"provider",
								"region",
							]);
						let region = "";
						if (options && options.region) {
							region = options.region;
						} else if(configuredRegion){
							region = configuredRegion;
						} else if(params.length > 1) {
							region = params[1];
						}
						const tableName = params[0];
						serverless.cli.log(
							`Fetching Streams of [ Table : ${tableName} ] in region ${region}`
						);
						const profile = (options && options['aws-profile']) ||
							(serverless.service && serverless.service.provider && serverless.service.provider.profile) ||
							process.env.AWS_PROFILE;
						const data = await getDynamoDBStreams(
							region,
							tableName,
							buildCredentials(profile)
						);

						myStreamArn = extractStreamARNFromStreamData(data, tableName);
						if (!myStreamArn)
							throw new Error('Could not find stream of this Table');
						else {
							serverless.cli.log(
								`Fetched stream of [ Table : ${tableName} ] => ${myStreamArn}`
							);
						}
					} catch (err) {
						const errorMessage =
							err && err.message ? err.message : err;
						serverless.cli.log(
							`Error: ${JSON.stringify(errorMessage)}`
						);
					}

					return {
						value: myStreamArn,
					};
				},
			},
		};
	}
}

const extractStreamARNFromStreamData = (data, tableName) => {
	const Streams = data.Streams;
	if (!Streams || Streams.length === 0) {
		throw new Error(
			`Cannot Find Stream of [ Table : ${tableName} ], make sure the table exist and stream is enabled`
		);
	}
	const streamArn = Streams[0].StreamArn;
	return streamArn;
};

const buildCredentials = (profile) => profile ? new AWS.SharedIniFileCredentials({ profile }) : undefined;

const getDynamoDBStreams = async (region, tableName, credentials) => {
	const dynamoStreams = new AWS.DynamoDBStreams({
		region,
		credentials,
	});
	const params = {
		TableName: tableName,
	};
	return dynamoStreams.listStreams(params).promise();
};

module.exports = fetchDynamoDBStreamsPlugin;
