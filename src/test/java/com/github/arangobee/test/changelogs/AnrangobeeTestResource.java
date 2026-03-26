package com.github.arangobee.test.changelogs;

import com.arangodb.ArangoDatabase;
import com.github.arangobee.changeset.ChangeLog;
import com.github.arangobee.changeset.ChangeSet;

/**
 * @author Christophe Moine
 * @since 11/03/2019
 */
@ChangeLog(order = "1")
public class AnrangobeeTestResource {
    @ChangeSet(author = "testuser", id = "test1", order = "01")
    public void testChangeSet() {
        System.out.println("invoked 1");
    }

    @ChangeSet(author = "testuser", id = "test2", order = "02")
    public void testChangeSet2() {
        System.out.println("invoked 2");
    }

    @ChangeSet(author = "testuser", id = "test3", order = "03")
    public void testChangeSet5(ArangoDatabase arangoDatabase) {
        System.out.println("invoked 5 with arangoDatabase=" + arangoDatabase.toString());
    }
}
