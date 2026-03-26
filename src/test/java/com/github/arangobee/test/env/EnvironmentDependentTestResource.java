package com.github.arangobee.test.env;

import com.github.arangobee.changeset.ChangeLog;
import com.github.arangobee.changeset.ChangeSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import static org.junit.jupiter.api.Assertions.assertNotNull;


@ChangeLog(order = "3")
public class EnvironmentDependentTestResource {

    @Autowired
    private Environment env;


    @ChangeSet(author = "testuser", id = "Envtest1", order = "01")
    public void testChangeSet7WithEnvironment() {
        System.out.println("invoked Envtest1 with Environment " + env);
        assertNotNull(env);
    }

}
