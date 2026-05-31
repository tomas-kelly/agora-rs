#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { FoodPreferencesStack } from '../lib/food-preferences-stack';

const app = new cdk.App();
const env = app.node.tryGetContext('env') || 'dev';

new FoodPreferencesStack(app, `FoodPreferences-${env}`, {
  env: {
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1',
    account: process.env.CDK_DEFAULT_ACCOUNT,
  },
});
