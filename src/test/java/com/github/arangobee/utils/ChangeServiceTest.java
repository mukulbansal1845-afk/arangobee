package com.github.arangobee.utils;

import com.github.arangobee.changeset.ChangeEntry;
import com.github.arangobee.exception.ArangobeeChangeSetException;
import com.github.arangobee.test.changelogs.AnotherArangobeeTestResource;
import com.github.arangobee.test.changelogs.AnrangobeeTestResource;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


/**
 * @author lstolowski
 * @since 27/07/2014
 */
public class ChangeServiceTest {

    @Test
    public void shouldFindChangeLogClasses() {
        // given
        String scanPackage = AnrangobeeTestResource.class.getPackage().getName();
        ChangeService service = new ChangeService(scanPackage);

        // when
        List<Class<?>> foundClasses = service.fetchChangeLogs();

        // then
        assertThat(foundClasses).isNotNull();
        assertThat(foundClasses).hasSizeGreaterThan(0);
    }

    @Test
    public void shouldFindChangeSetMethods() throws ArangobeeChangeSetException {
        // given
        String scanPackage = AnrangobeeTestResource.class.getPackage().getName();
        ChangeService service = new ChangeService(scanPackage);

        // when
        List<Method> foundMethods = service.fetchChangeSets(AnrangobeeTestResource.class);

        // then
        assertThat(foundMethods).isNotNull();
        assertThat(foundMethods).hasSize(3);
    }

    @Test
    public void shouldFindAnotherChangeSetMethods() throws ArangobeeChangeSetException {
        // given
        String scanPackage = AnotherArangobeeTestResource.class.getPackage().getName();
        ChangeService service = new ChangeService(scanPackage);

        // when
        List<Method> foundMethods = service.fetchChangeSets(AnotherArangobeeTestResource.class);

        // then
        assertThat(foundMethods).isNotNull();
        assertThat(foundMethods).hasSize(3);
    }

    @Test
    public void shouldFindIsRunAlwaysMethod() throws ArangobeeChangeSetException {
        // given
        String scanPackage = AnrangobeeTestResource.class.getPackage().getName();
        ChangeService service = new ChangeService(scanPackage);

        // when
        List<Method> foundMethods = service.fetchChangeSets(AnotherArangobeeTestResource.class);

        // then
        for (Method foundMethod : foundMethods) {
            if (foundMethod.getName().equals("testChangeSetWithAlways")) {
                assertThat(service.isRunAlwaysChangeSet(foundMethod)).isTrue();
            } else {
                assertThat(service.isRunAlwaysChangeSet(foundMethod)).isFalse();
            }
        }
    }

    @Test
    public void shouldCreateEntry() throws ArangobeeChangeSetException {
        // given
        String scanPackage = AnrangobeeTestResource.class.getPackage().getName();
        ChangeService service = new ChangeService(scanPackage);
        List<Method> foundMethods = service.fetchChangeSets(AnrangobeeTestResource.class);

        for (Method foundMethod : foundMethods) {
            // when
            ChangeEntry entry = service.createChangeEntry(foundMethod);

            // then
            assertThat(entry.getAuthor()).isEqualTo("testuser");
            assertThat(entry.getChangeLogClass()).isEqualTo(AnrangobeeTestResource.class.getName());
            assertThat(entry.getTimestamp()).isNotNull();
            assertThat(entry.getChangeId()).isNotNull();
            assertThat(entry.getChangeSetMethodName()).isNotNull();
        }
    }

    @Test
    public void shouldFailOnDuplicatedChangeSets() {
        String scanPackage = ChangeLogWithDuplicate.class.getPackage().getName();
        ChangeService service = new ChangeService(scanPackage);

        assertThatThrownBy(() -> service.fetchChangeSets(ChangeLogWithDuplicate.class))
            .isInstanceOf(ArangobeeChangeSetException.class);
    }

}
