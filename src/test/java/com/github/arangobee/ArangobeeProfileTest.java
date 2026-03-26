package com.github.arangobee;

import com.github.arangobee.changeset.ChangeEntry;
import com.github.arangobee.exception.ArangobeeConnectionException;
import com.github.arangobee.exception.ArangobeeLockException;
import com.github.arangobee.resources.EnvironmentMock;
import com.github.arangobee.test.changelogs.AnotherArangobeeTestResource;
import com.github.arangobee.test.profiles.def.UnProfiledChangeLog;
import com.github.arangobee.test.profiles.dev.ProfiledDevChangeLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


/**
 * Tests for Spring profiles integration
 *
 * @author lstolowski
 * @since 2014-09-17
 */
@ExtendWith(MockitoExtension.class)
public class ArangobeeProfileTest extends AbstractArangobeeTest {
    public static final int CHANGELOG_COUNT = 6;

    @BeforeEach
    @Override
    public void before() throws ArangobeeConnectionException, ArangobeeLockException {
        super.before();
        when(dao.acquireProcessLock()).thenReturn(FAKE_LOCK);
    }

    @Test
    public void shouldRunDevProfileAndNonAnnotated() throws Exception {
        // given
        setSpringEnvironment(new EnvironmentMock("dev", "test"));
        runner.setChangeLogsScanPackage(ProfiledDevChangeLog.class.getPackage().getName());
        when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(true);

        // when
        runner.execute();

        // then
        assertEquals(1, count("Pdev1", "testuser"));  //  no-@Profile  should not match

        assertEquals(1, count("Pdev4", "testuser"));  //  @Profile("dev")  should not match

        assertEquals(0, count("Pdev3", "testuser"));  //  @Profile("default")  should not match
    }

    @Test
    public void shouldRunUnprofiledChangeLog() throws Exception {
        // given
        setSpringEnvironment(new EnvironmentMock("test"));
        runner.setChangeLogsScanPackage(UnProfiledChangeLog.class.getPackage().getName());
        when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(true);

        // when
        runner.execute();

        // then
        assertEquals(1, count("Pdev1", "testuser"));

        assertEquals(1, count("Pdev2", "testuser"));

        assertEquals(1, count("Pdev3", "testuser"));  //  @Profile("dev")  should not match

        assertEquals(0, count("Pdev4", "testuser"));  //  @Profile("pro")  should not match

        assertEquals(1, count("Pdev5", "testuser"));  //  @Profile("!pro")  should match
    }

    @Test
    public void shouldNotRunAnyChangeSet() throws Exception {
        // given
        setSpringEnvironment(new EnvironmentMock("foobar"));
        runner.setChangeLogsScanPackage(ProfiledDevChangeLog.class.getPackage().getName());

        // when
        runner.execute();

        // then
        assertEquals(0, count(null, null));
    }

    @Test
    public void shouldRunChangeSetsWhenNoEnv() throws Exception {
        // given
        setSpringEnvironment(null);
        runner.setChangeLogsScanPackage(AnotherArangobeeTestResource.class.getPackage().getName());
        when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(true);

        // when
        runner.execute();

        // then
        assertEquals(CHANGELOG_COUNT, count(null, null));
    }

    @Test
    public void shouldRunChangeSetsWhenEmptyEnv() throws Exception {
        // given
        setSpringEnvironment(new EnvironmentMock());
        runner.setChangeLogsScanPackage(AnotherArangobeeTestResource.class.getPackage().getName());
        when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(true);

        // when
        runner.execute();

        // then
        assertEquals(CHANGELOG_COUNT, count(null, null));
    }

    @Test
    public void shouldRunAllChangeSets() throws Exception {
        // given
        setSpringEnvironment(new EnvironmentMock("dev"));
        runner.setChangeLogsScanPackage(AnotherArangobeeTestResource.class.getPackage().getName());
        when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(true);

        // when
        runner.execute();

        // then
        assertEquals(CHANGELOG_COUNT, count(null, null));
    }
}
