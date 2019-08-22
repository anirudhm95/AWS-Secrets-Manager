const AWS = require('aws-sdk');
const logger = require('./modules/log');
const mysql = require('mysql');
const config = require('./modules/config');
const error = require('./modules/error');


/* This handler uses the single-user rotation scheme to rotate an RDS MySQL user credential.
   This rotation scheme logs into the database as the user and rotates the user's own password, immediately invalidating the user's previous password.

    Args: event (dict/object): Lambda dictionary of event parameters. These keys must include the following:
            - SecretId: The secret ARN or identifier
            - ClientRequestToken: The ClientRequestToken of the secret version
            - Step: The rotation step (one of createSecret, setSecret, testSecret, or finishSecret)

          context (LambdaContext): The Lambda runtime information
*/
exports.handler = async (event, context) => {
  logger.log('info:handler:init', event);
  const endpoint = 'https://secretsmanager.us-east-1.amazonaws.com',
    region = 'us-east-1';

  // Create a Secrets Manager client
  const secretManagerClient = new AWS.SecretsManager({
    endpoint,
    region,
  });

  const step = event.Step;
  logger.log('info:handler:step', step);
  const token = event.ClientRequestToken;
  logger.log('info:handler:token', token);
  const arn = event.SecretId;

  switch (step) {
    case 'createSecret':
      const successResponse = await createSecret(secretManagerClient, token);
      // logger.log('info:handler:successResponse', successResponse);
      break;
    case 'setSecret':
      const successInChangeRDSpassword = await setSecret(secretManagerClient, token);
      // logger.log('info:handler:successInChangeRDSpassword', successInChangeRDSpassword);
      break;
    case 'testSecret':
      await testSecret(secretManagerClient, token);
      break;
    case 'finishSecret':
      await finishSecret(secretManagerClient, token);
      break;
    default:
  }
};

/* Generate a new secret.
   This method first checks for the existence of a secret for the passed in token. If one does not exist, it will generate a
   new secret and put it with the passed in token.

   Args: (secretManagerClient (client): The secrets manager service client, token (string): The ClientRequestToken associated with the secret version)
*/
async function createSecret(secretManagerClient, token) {
  logger.log('info:createSecret:init');
  const secretName = config.secretsmanager.name;
  let params = {
    SecretId: secretName,
  };

  let currentSecretValue = await secretManagerClient.getSecretValue(params).promise();
  logger.log('info:createSecret:currentSecretValue', currentSecretValue);
  let secretString = currentSecretValue.SecretString;
  logger.log('info:createSecret:secretString:', secretString);
  let secretStringObject = JSON.parse(secretString);
  let password = secretStringObject.password;
  logger.log('info:createSecret:password: ', password);

  const describesecret = await secretManagerClient.describeSecret(params).promise();
  logger.log('info:createSecret:describesecret: ', describesecret);

  try {
    const gettingSecret = await secretManagerClient.getSecretValue({
      SecretId: secretName,
      VersionId: token,
    }).promise();
    logger.log('info:createSecret:AWSPENDING secret: ', gettingSecret);
    logger.log('info:createSecret:', 'Successfully retrieved secret for AWSPENDING');
  } catch (err) {
    const createRandomPassword = await secretManagerClient.getRandomPassword({ ExcludeCharacters: '/@"\'\\' }).promise();
    let randomPassword = createRandomPassword.RandomPassword;
    logger.log('info:Random Password: ', randomPassword);
    secretStringObject.password = randomPassword;
    let newSecretString = JSON.stringify(secretStringObject);
    logger.log('info:createSecret:newSecretString: ', newSecretString);

    try {
      logger.log('info:createSecret', 'Starting to add secret for ARN and version.');
      return await secretManagerClient.putSecretValue({
        SecretId: secretName,
        ClientRequestToken: token,
        SecretString: newSecretString,
        VersionStages: ['AWSPENDING'],
      }).promise();
    } catch (err) {
      logger.log('error:createSecret', 'ERROR in put secret for ARN and version.', err);
    }
  }
}

/* Set the pending secret in the database.
   This method gets the AWSPENDING secret and modifys with the database with the AWSPENDING secret.

   Args: (secretManagerClient (client): The secrets manager service client, token (string): The ClientRequestToken associated with the secret version)
*/
async function setSecret(secretManagerClient, token) {
  const gettingSecret = await secretManagerClient.getSecretValue({
    SecretId: config.secretsmanager.name,
    VersionId: token,
  }).promise();
  logger.log('info:setSecret:WHTSECRETinAWSPENDING: ', gettingSecret);
  let secretString = gettingSecret.SecretString;
  // logger.log('info:setSecret:secretString:', secretString);
  let secretStringObject = JSON.parse(secretString);
  let newPassword = secretStringObject.password;
  logger.log('info:setSecret:password: ', newPassword);

  const rds = new AWS.RDS();
  let rdsCred = {
    DBClusterIdentifier: 'phat-cluster',
    ApplyImmediately: true,
    MasterUserPassword: newPassword,
  };

  try {
    let data = await rds.modifyDBCluster(rdsCred).promise();
    logger.log('info:RDS', 'Successfully modified password for RDS');
    logger.log('info:RDS', data);
  } catch (err) {
    logger.log('error:modifyDBCluster', 'ERROR in modify password in RDS.', err);
  }

  try {
    await mysql.createConnection({
      host: 'phat-cluster.cluster-cbimfz1gncny.us-east-1.rds.amazonaws.com',
      user: 'phat1234',
      password: newPassword,
      port: 3306,
      connect_timeout: 5,
    });
  }
  catch (err) {
    logger.log('error:testingSecret', 'Error in connecting to database', err);
  }
}

/* Gets a connection to MySQL DB from a secret dictionary.
   Function tries to connect to the database grabbing connection info from the secret dictionary.

   Args:  secret_dict (dict/object): The Secret Dictionary
*/
async function getConnection(secretStingObject) {
  logger.log('info:getConnection', 'Creating Connection to Database');
  try {
    return await mysql.createConnection({
      host: secretStingObject.host,
      user: secretStingObject.username,
      password: secretStingObject.password,
      port: secretStingObject.port,
      connect_timeout: 5,
    });
  }
  catch (err) {
    logger.log('error:getConnection', 'Error in connecting to database', err);
    return null;
  }
}

/* Test the pending secret against the database.
   This method tries to log into the database with the secrets staged with AWSPENDING.

   Args: (secretManagerClient (client): The secrets manager service client, token (string): The ClientRequestToken associated with the secret version)
*/
async function testSecret(secretManagerClient, token) {
  try {
    const testPendingSecret = await secretManagerClient.getSecretValue({
      SecretId: config.secretsmanager.name,
      VersionId: token,
    }).promise();
    logger.log('info:testSecret:AWSPENDING Secret: ', testPendingSecret);
    let secretString = testPendingSecret.SecretString;
    logger.log('info:testSecret:secretString:', secretString);
    let secretStringObject = JSON.parse(secretString);
    getConnection(secretStringObject);
  } catch (err) {
    logger.log('error:testSecret', 'Error in connecting to secretsManger', err);
  }
}

/* Finish the rotation by marking the pending secret as current.
   This method finishes the secret rotation by staging the secret staged AWSPENDING with the AWSCURRENT stage.

   Args: (secretManagerClient (client): The secrets manager service client, token (string): The ClientRequestToken associated with the secret version)
*/
async function finishSecret(secretManagerClient, token) {
  const metadata = await secretManagerClient.describeSecret({ SecretId: config.secretsmanager.name }).promise();
  // logger.log('info:finishSecret:describesecret: ', metadata);

  let allVersion = metadata.VersionIdsToStages;
  logger.log('info:finishSecret:AllVersion: ', allVersion);

  let curretVersion;
  for (const [key, value] of Object.entries(allVersion)) {
    if (value[0] === 'AWSCURRENT') {
      curretVersion = key;
    }
  }
  logger.log('info:finishSecret:curretVersion ', curretVersion);

  await secretManagerClient.updateSecretVersionStage({
    SecretId: config.secretsmanager.name,
    VersionStage: 'AWSCURRENT',
    MoveToVersionId: token,
    RemoveFromVersionId: curretVersion,
  }).promise();
  logger.log('info:finishSecret', 'Successfully set AWSCURRENT stage to version for secret.');

  const describesecret = await secretManagerClient.describeSecret({ SecretId: config.secretsmanager.name }).promise();
  logger.log('info:finishSecret:describesecret: ', describesecret);
}