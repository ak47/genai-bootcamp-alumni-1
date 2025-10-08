# SuperDataScience Alumni Club
## Extending AI with MCP

# Configuring Github Actions

You can deploy projects from GitHub actions. You must run from your
own fork to do so.

## Set up

1.  **Deploy the CloudFormation stack**

**Skip the CloudFormation deployment if you still have the role from the bootcamp**

    This command will create the IAM role that GitHub Actions will use to deploy your stacks.
    Replace `<your-org>` and `<your-repo>` with your GitHub organization and repository name.
```bash
aws cloudformation deploy \
    --template-file github-actions.yaml \
    --stack-name github-actions-role \
    --parameter-overrides GitHubOrg=<your-org> GitHubRepo=<your-repo> \
    --capabilities CAPABILITY_NAMED_IAM
```

After deployment, get the `RoleArn` output from the stack.

```bash
aws cloudformation describe-stacks \
    --stack-name github-actions-role \
    --query "Stacks[0].Outputs[?OutputKey=='RoleArn'].OutputValue" \
    --output text
```

2.  **Set the `AWS_ROLE_ARN` in GitHub**

The `RoleArn` from the previous step needs to be configured as a repository variable in GitHub.

- Navigate to your repository on GitHub.
- Go to `Settings` > `Secrets and variables` > `Actions`.
- Click `New repository secret`.
- Name the variable `AWS_ROLE_ARN`.
- Paste the `RoleArn` value from the previous step.
- Click `Add variable`.

## Run a deployment from GitHub Actions

- Navigate to the `Actions` tab in your GitHub repository.
- In the left sidebar, select the workflow you want to run (e.g., `Deploy Hello World`).
- Click the `Run workflow` dropdown, and then click the `Run workflow` button to start the deployment.
