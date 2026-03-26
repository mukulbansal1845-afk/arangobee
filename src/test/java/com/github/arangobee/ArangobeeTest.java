package com.github.arangobee;

import com.github.arangobee.changeset.ChangeEntry;
import com.github.arangobee.exception.ArangobeeConfigurationException;
import com.github.arangobee.exception.ArangobeeConnectionException;
import com.github.arangobee.exception.ArangobeeException;
import com.github.arangobee.exception.ArangobeeLockException;
import com.github.arangobee.test.changelogs.AnrangobeeTestResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


@ExtendWith(MockitoExtension.class)
public class ArangobeeTest extends AbstractArangobeeTest {

    @BeforeEach
    @Override
    public void before() throws ArangobeeConnectionException, ArangobeeLockException {
        super.before();
        runner.setChangeLogsScanPackage(AnrangobeeTestResource.class.getPackage().getName());
        lenient().when(dao.acquireProcessLock()).thenReturn(FAKE_LOCK);
    }

    @Test()
    public void shouldThrowAnExceptionIfNoDbSet() {
        assertThatThrownBy(() -> new Arangobee(null, null).execute())
            .isInstanceOf(ArangobeeConfigurationException.class);
    }

    @Test
    public void shouldExecuteAllChangeSets() throws Exception {
        // given
        when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(true);

        // when
        runner.execute();

        // then
        verify(dao, times(6)).save(any(ChangeEntry.class));

        // dbchangelog collection checking
        assertEquals(1, count("test1", "testuser"));
        assertEquals(1, count("test2", "testuser"));
        assertEquals(1, count("test3", "testuser"));
        assertEquals(6, count(null, "testuser"));
    }

    @Test
    public void shouldPassOverChangeSets() throws Exception {
        // given
        when(dao.isNewChange(any(ChangeEntry.class))).thenReturn(false);

        // when
        runner.execute();

        // then
        verify(dao, times(0)).save(any(ChangeEntry.class)); // no changesets saved to dbchangelog
    }

    @Test
    public void shouldExecuteProcessWhenLockAcquired() throws Exception {
        // given
        when(dao.acquireProcessLock()).thenReturn(FAKE_LOCK);

        // when
        runner.execute();

        // then
        verify(dao, atLeastOnce()).isNewChange(any(ChangeEntry.class));
    }

    @Test
    public void shouldReleaseLockAfterWhenLockAcquired() throws Exception {
        // given
        when(dao.acquireProcessLock()).thenReturn(FAKE_LOCK);

        // when
        runner.execute();

        // then
        verify(dao).releaseProcessLock(FAKE_LOCK);
    }

    @Test
    public void shouldNotExecuteProcessWhenLockNotAcquired() throws ArangobeeConnectionException, ArangobeeLockException {
        // given
        when(dao.acquireProcessLock()).thenReturn(null);

        // when
        assertThatThrownBy(() -> runner.execute())
            .isInstanceOf(ArangobeeException.class);

        // then
        verify(dao, never()).isNewChange(any(ChangeEntry.class));
    }

    @Test
    public void shouldReturnExecutionStatusBasedOnDao() throws Exception {
        // given
        when(dao.isProccessLockHeld()).thenReturn(true);

        boolean inProgress = runner.isExecutionInProgress();

        // then
        assertTrue(inProgress);
    }

    @Test
    public void shouldReleaseLockWhenExceptionInMigration() throws Exception {
        // given
        // would be nicer with a mock for the whole execution, but this would mean breaking out to separate class..
        // this should be "good enough"
        when(dao.acquireProcessLock()).thenReturn(FAKE_LOCK);
        when(dao.isNewChange(any(ChangeEntry.class))).thenThrow(RuntimeException.class);

        // when
        // have to catch the exception to be able to verify after
        try {
            runner.execute();
        } catch (Exception e) {
            // do nothing
        }
        // then
        verify(dao).releaseProcessLock(FAKE_LOCK);

    }
}
