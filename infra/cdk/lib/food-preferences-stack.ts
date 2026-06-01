import * as cdk from 'aws-cdk-lib';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as cognito from 'aws-cdk-lib/aws-cognito';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import { Construct } from 'constructs';

export class FoodPreferencesStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // DynamoDB Table
    const table = new dynamodb.Table(this, 'FoodPreferences', {
      partitionKey: { name: 'userId', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'preferenceKey', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecovery: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    // Cognito User Pool
    const userPool = new cognito.UserPool(this, 'UserPool', {
      selfSignUpEnabled: true,
      signInAliases: { email: true },
      autoVerify: { email: true },
      passwordPolicy: { minLength: 8, requireUppercase: true, requireDigits: true, requireSymbols: true },
    });

    const userPoolClient = new cognito.UserPoolClient(this, 'UserPoolClient', {
      userPool,
      authFlows: { userPassword: true, userSrp: true },
      generateSecret: false,
    });

    // Lambda Function
    const handler = new lambda.Function(this, 'ApiHandler', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'handlers/index.handler',
      code: lambda.Code.fromAsset('../../src', {
        exclude: ['node_modules', '__tests__', '*.test.ts', 'tsconfig.json', 'package-lock.json'],
      }),
      memorySize: 256,
      timeout: cdk.Duration.seconds(10),
      environment: {
        TABLE_NAME: table.tableName,
      },
      tracing: lambda.Tracing.ACTIVE,
    });

    // Least-privilege DynamoDB access
    table.grant(handler, 'dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:Query');

    // API Gateway
    const api = new apigateway.RestApi(this, 'FoodPreferencesApi', {
      restApiName: 'Food Preferences',
      deployOptions: { tracingEnabled: true },
    });

    const authorizer = new apigateway.CognitoUserPoolsAuthorizer(this, 'CognitoAuthorizer', {
      cognitoUserPools: [userPool],
    });

    const authOptions: apigateway.MethodOptions = {
      authorizer,
      authorizationType: apigateway.AuthorizationType.COGNITO,
    };

    const integration = new apigateway.LambdaIntegration(handler);

    const users = api.root.addResource('users');
    const userId = users.addResource('{userId}');
    const preferences = userId.addResource('preferences');
    const favoriteMeal = preferences.addResource('favorite-meal');

    favoriteMeal.addMethod('PUT', integration, authOptions);
    favoriteMeal.addMethod('GET', integration, authOptions);

    // Alarms
    new cloudwatch.Alarm(this, 'LambdaErrorAlarm', {
      metric: handler.metricErrors({ period: cdk.Duration.minutes(5) }),
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      alarmDescription: 'Lambda error rate exceeded threshold',
    });

    new cloudwatch.Alarm(this, 'ApiLatencyAlarm', {
      metric: api.metricLatency({ period: cdk.Duration.minutes(5), statistic: 'p95' }),
      threshold: 200,
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      alarmDescription: 'API Gateway p95 latency exceeded 200ms',
    });

    // Outputs
    new cdk.CfnOutput(this, 'ApiUrl', { value: api.url });
    new cdk.CfnOutput(this, 'UserPoolId', { value: userPool.userPoolId });
    new cdk.CfnOutput(this, 'UserPoolClientId', { value: userPoolClient.userPoolClientId });
    new cdk.CfnOutput(this, 'TableName', { value: table.tableName });
  }
}
