/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.validation.json.JsonValidationException;
import io.airbyte.workers.exception.RecordSchemaValidationException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.ImmutablePair;

/**
 * Validates that AirbyteRecordMessage data conforms to the JSON schema defined by the source's
 * configured catalog
 */
public class RecordSchemaValidator {

  private static final JsonSchemaValidator validator = new JsonSchemaValidator();
  private final ExecutorService validationExecutor;
  private final Map<AirbyteStreamNameNamespacePair, JsonNode> streams;
  private final boolean backgroundValidation;

  /**
   * @param streamNamesToSchemas
   * @param backgroundValidation Pass true if the json schema validation should occur in a different
   *        thread. Pass false if the json schema validation should occur in current thread. TODO:
   *        remove the backgroundValidation parameter when PerfBackgroundJsonValidation feature-flag
   *        is removed.
   */
  public RecordSchemaValidator(final Map<AirbyteStreamNameNamespacePair, JsonNode> streamNamesToSchemas, final boolean backgroundValidation) {
    this(streamNamesToSchemas, backgroundValidation, Executors.newFixedThreadPool(1));
  }

  @VisibleForTesting
  RecordSchemaValidator(final Map<AirbyteStreamNameNamespacePair, JsonNode> streamNamesToSchemas,
                        final boolean backgroundValidation,
                        final ExecutorService validationExecutor) {
    this.backgroundValidation = backgroundValidation;
    // streams is Map of a stream source namespace + name mapped to the stream schema
    // for easy access when we check each record's schema
    this.streams = streamNamesToSchemas;
    this.validationExecutor = validationExecutor;
    // initialize schema validator to avoid creating validators each time.
    for (final AirbyteStreamNameNamespacePair stream : streamNamesToSchemas.keySet()) {
      // We must choose a JSON validator version for validating the schema
      // Rather than allowing connectors to use any version, we enforce validation using V7
      final var schema = streams.get(stream);
      ((ObjectNode) schema).put("$schema", "http://json-schema.org/draft-07/schema#");
      validator.initializeSchemaValidator(stream.toString(), schema);
    }
  }

  /**
   * Takes an AirbyteRecordMessage and uses the JsonSchemaValidator to validate that its data conforms
   * to the stream's schema If it does not, this method throws a RecordSchemaValidationException
   *
   * @throws RecordSchemaValidationException
   */
  public void validateSchema(
                             final AirbyteRecordMessage message,
                             final AirbyteStreamNameNamespacePair messageStream,
                             final ConcurrentHashMap<AirbyteStreamNameNamespacePair, ImmutablePair<Set<String>, Integer>> validationErrors) {
    if (backgroundValidation) {
      validationExecutor.execute(() -> {
        try {
          doValidateSchema(message, messageStream);
        } catch (final RecordSchemaValidationException e) {
          handleException(e, messageStream, validationErrors);
        }
      });
    } else {
      try {
        doValidateSchema(message, messageStream);
      } catch (final RecordSchemaValidationException e) {
        handleException(e, messageStream, validationErrors);
      }
    }
  }

  private void handleException(final RecordSchemaValidationException e,
                               final AirbyteStreamNameNamespacePair messageStream,
                               final ConcurrentHashMap<AirbyteStreamNameNamespacePair, ImmutablePair<Set<String>, Integer>> validationErrors) {
    final ImmutablePair<Set<String>, Integer> exceptionWithCount = validationErrors.get(messageStream);
    if (exceptionWithCount == null) {
      validationErrors.put(messageStream, new ImmutablePair<>(e.errorMessages, 1));
    } else {
      final Integer currentCount = exceptionWithCount.getRight();
      final Set<String> currentErrorMessages = exceptionWithCount.getLeft();
      final Set<String> updatedErrorMessages = Stream.concat(currentErrorMessages.stream(), e.errorMessages.stream())
          .collect(Collectors.toSet());
      validationErrors.put(messageStream, new ImmutablePair<>(updatedErrorMessages, currentCount + 1));
    }
  }

  /**
   * @throws RecordSchemaValidationException
   */
  private void doValidateSchema(final AirbyteRecordMessage message, final AirbyteStreamNameNamespacePair messageStream) {
    final JsonNode messageData = message.getData();
    final JsonNode matchingSchema = streams.get(messageStream);

    try {
      validator.ensureInitializedSchema(messageStream.toString(), messageData);
    } catch (final JsonValidationException e) {
      final List<String[]> invalidRecordDataAndType = validator.getValidationMessageArgs(matchingSchema, messageData);
      final List<String> invalidFields = validator.getValidationMessagePaths(matchingSchema, messageData);

      final Set<String> validationMessagesToDisplay = new HashSet<>();
      for (int i = 0; i < invalidFields.size(); i++) {
        final StringBuilder expectedType = new StringBuilder();
        if (invalidRecordDataAndType.size() > i && invalidRecordDataAndType.get(i).length > 1) {
          expectedType.append(invalidRecordDataAndType.get(i)[1]);
        }
        final StringBuilder newMessage = new StringBuilder();
        newMessage.append(invalidFields.get(i));
        newMessage.append(" is of an incorrect type.");
        if (expectedType.length() > 0) {
          newMessage.append(" Expected it to be " + expectedType);
        }
        validationMessagesToDisplay.add(newMessage.toString());
      }

      throw new RecordSchemaValidationException(validationMessagesToDisplay,
          String.format("Record schema validation failed for %s", messageStream), e);
    }
  }

}
