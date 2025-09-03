FROM public.ecr.aws/lambda/java:21

COPY build/libs/etl-lambda.jar /function/etl-lambda.jar

CMD ["com.example.lambda.ImportLambda::handleRequest"]
