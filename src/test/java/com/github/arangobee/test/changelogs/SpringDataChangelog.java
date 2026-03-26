package com.github.arangobee.test.changelogs;

import com.arangodb.springframework.core.template.ArangoTemplate;
import com.github.arangobee.changeset.ChangeLog;
import com.github.arangobee.changeset.ChangeSet;

/**
 * @author abelski
 */
@ChangeLog
public class SpringDataChangelog {
    @ChangeSet(author = "abelski", id = "spring_test4", order = "04")
    public void testChangeSet(ArangoTemplate arangoTemplate) {
        System.out.println("invoked  with arangoTemplate=" + arangoTemplate.toString());
    }
}
