/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.cdap.etl.mock.action;

import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

public class MockCustomActionContext extends MockActionContext {
  @Override
  public FailureCollector getFailureCollector() {
    return new FailureCollector() {

      private static final String STAGE = "stage";
      private final List<ValidationFailure> failures = new ArrayList<>();
      private final String stageName = "testStage";

      @Override
      public ValidationFailure addFailure(String message, @Nullable String correctiveAction) {
        ValidationFailure failure = new ValidationFailure(message, correctiveAction);
        failures.add(failure);
        return failure;
      }

      @Override
      public ValidationException getOrThrowException() throws ValidationException {
        if (failures.isEmpty()) {
          return new ValidationException(failures);
        }
        for (ValidationFailure failure : failures) {
          List<ValidationFailure.Cause> causes = failure.getCauses();
          if (causes.isEmpty()) {
            causes.add(new ValidationFailure.Cause().addAttribute(STAGE, stageName));
            continue;
          }
          for (ValidationFailure.Cause cause : causes) {
            // stage name is added by the configurer before throwing the validation exception
            cause.addAttribute(STAGE, stageName);
          }
        }

        throw new ValidationException(failures);
      }

      /**
       * Returns a list of failures.
       */
      public List<ValidationFailure> getValidationFailures() {
        return failures;
      }
    };
  }
}
