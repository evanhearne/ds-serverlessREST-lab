import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand } from "@aws-sdk/lib-dynamodb";
import { QueryCommand, QueryCommandInput } from "@aws-sdk/lib-dynamodb"; // For querying cast members

const ddbDocClient = createDDbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {     
  try {
    console.log("[EVENT]", JSON.stringify(event));

    // Extract movieId from path parameters
    const parameters = event?.pathParameters;
    const movieId = parameters?.movieId ? parseInt(parameters.movieId) : undefined;

    if (!movieId) {
      return {
        statusCode: 404,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ Message: "Missing or invalid movieId" }),
      };
    }

    // Check if cast information is requested via query string
    const queryStringParameters = event?.queryStringParameters;
    const includeCast = queryStringParameters?.cast === "true";  // Check for cast=true

    // Fetch movie metadata from DynamoDB
    const movieData = await ddbDocClient.send(
      new GetCommand({
        TableName: process.env.TABLE_NAME,
        Key: { id: movieId },
      })
    );

    if (!movieData.Item) {
      return {
        statusCode: 404,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ Message: "Movie not found" }),
      };
    }

    // Prepare the response data
    let responseBody = { data: { ...movieData.Item } };

    // If cast=true, query the cast members and include in the response
    if (includeCast) {
      const castData = await getCastForMovie(movieId);
      if (castData && castData.length > 0) {
        responseBody.data.cast = castData;
      } else {
        responseBody.data.cast = "No cast information found";  // In case no cast data is available
      }
    }

    // Return the final response with or without cast data
    return {
      statusCode: 200,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(responseBody),
    };

  } catch (error: any) {
    console.log(JSON.stringify(error));
    return {
      statusCode: 500,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ error }),
    };
  }
};

// Function to fetch cast members for a specific movie
async function getCastForMovie(movieId: number) {
  const commandInput: QueryCommandInput = {
    TableName: process.env.CAST_TABLE_NAME,  // Cast table environment variable
    KeyConditionExpression: "movieId = :m",
    ExpressionAttributeValues: {
      ":m": movieId,
    },
  };

  const commandOutput = await ddbDocClient.send(new QueryCommand(commandInput));
  return commandOutput.Items || [];
}

function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
