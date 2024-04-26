from aws_cdk import (
    Duration,
    Stack,
    aws_lambda,
    aws_lambda_event_sources as lambda_event_sources,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_s3_notifications as s3n,
    aws_events as events,
    aws_events_targets as targets
)

from constructs import Construct

class CdkAppStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        data_ingest_func = aws_lambda.Function(
            self,
            id = "data_ingest_func",
            code = aws_lambda.Code.from_asset("./functions"),
            handler = "ingest.lambda_handler",
            runtime = aws_lambda.Runtime.PYTHON_3_11,
            timeout = Duration.seconds(300),
        )

        # Run daily at noon Zulu
        rule = events.Rule(
            self, "Rule",
            schedule = events.Schedule.cron(
                minute='0',
                hour='12',
                day='*',
                month='*',
                year='*'),
        )
        rule.add_target(targets.LambdaFunction(data_ingest_func))

        s3_bucket = s3.Bucket.from_bucket_arn(self, "rearc-quest-2024", "arn:aws:s3:::rearc-quest-2024")

        queue = sqs.Queue(
            self,
            "SQSMessenger",
            visibility_timeout = Duration.seconds(300),
        )

        s3_bucket.add_event_notification(s3.EventType.OBJECT_CREATED, s3n.SqsDestination(queue), s3.NotificationKeyFilter(prefix="Datasets-Part2/"))

        sqs_lambda = aws_lambda.Function(
            self,
            id = "data_analysis_func",
            code = aws_lambda.Code.from_asset("./functions"),
            handler = "analysis.lambda_handler",
            runtime = aws_lambda.Runtime.PYTHON_3_11,
            timeout = Duration.seconds(300),
        )

        sqs_event = lambda_event_sources.SqsEventSource(queue)

        sqs_lambda.add_event_source(sqs_event)