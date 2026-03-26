package com.github.arangobee.dao;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.model.CollectionCreateOptions;
import com.github.arangobee.changeset.ChangeEntry;
import com.github.arangobee.exception.ArangobeeConnectionException;
import com.github.arangobee.exception.ArangobeeLockException;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * @author Christophe Moine
 * @since 11/03/2019
 */
public class ChangeEntryDao {
    private static final Logger logger = LoggerFactory.getLogger("Arangobee dao");

    private ArangoDatabase arangoDatabase;
    private ChangeEntryIndexDao indexDao;
    private String changelogCollectionName;
    private boolean waitForLock;
    private long changeLogLockWaitTime;
    private long changeLogLockPollRate;
    private boolean throwExceptionIfCannotObtainLock;

    private LockDao lockDao;

    public ChangeEntryDao(String changelogCollectionName, String lockCollectionName, boolean waitForLock, long changeLogLockWaitTime,
                          long changeLogLockPollRate, boolean throwExceptionIfCannotObtainLock) {
        this.indexDao = new ChangeEntryIndexDao(changelogCollectionName);
        this.lockDao = new LockDao(lockCollectionName);
        this.changelogCollectionName = changelogCollectionName;
        this.waitForLock = waitForLock;
        this.changeLogLockWaitTime = changeLogLockWaitTime;
        this.changeLogLockPollRate = changeLogLockPollRate;
        this.throwExceptionIfCannotObtainLock = throwExceptionIfCannotObtainLock;
    }

    public ArangoDatabase getArangoDatabase() {
        return arangoDatabase;
    }

    public ArangoDatabase connectDb(ArangoDatabase arangoDatabase) {
        this.arangoDatabase = arangoDatabase;

        ensureChangeLogCollectionIndex(arangoDatabase, changelogCollectionName);
        initializeLock();
        return arangoDatabase;
    }

    /**
     * Try to acquire process lock
     *
     * @return true if successfully acquired, false otherwise
     * @throws ArangobeeConnectionException exception
     * @throws ArangobeeLockException       exception
     */
    public String acquireProcessLock() throws ArangobeeConnectionException, ArangobeeLockException {
        verifyDbConnection();
        String acquired = lockDao.acquireLock(getArangoDatabase());

        if (acquired == null && waitForLock) {
            long timeToGiveUp = new Date().getTime() + (changeLogLockWaitTime * 1000 * 60);
            while (acquired == null && new Date().getTime() < timeToGiveUp) {
                acquired = lockDao.acquireLock(getArangoDatabase());
                if (acquired == null) {
                    logger.info("Waiting for changelog lock....");
                    try {
                        Thread.sleep(changeLogLockPollRate * 1000);
                    } catch (InterruptedException e) {
                        // nothing
                    }
                }
            }
        }

        if (acquired == null && throwExceptionIfCannotObtainLock) {
            logger.info("Arangobee did not acquire process lock. Throwing exception.");
            throw new ArangobeeLockException("Could not acquire process lock");
        }

        return acquired;
    }

    public void releaseProcessLock(String lock) throws ArangobeeConnectionException {
        verifyDbConnection();
        lockDao.releaseLock(getArangoDatabase(), lock);
    }

    public boolean isProccessLockHeld() throws ArangobeeConnectionException {
        verifyDbConnection();
        return lockDao.isLockHeld(getArangoDatabase());
    }

    public boolean isNewChange(ChangeEntry changeEntry) throws ArangobeeConnectionException {
        verifyDbConnection();

        return !getArangoDatabase().query(
            "FOR t IN " + changelogCollectionName + " FILTER t." + ChangeEntry.KEY_CHANGEID + " == @" + ChangeEntry.KEY_CHANGEID + " && t."
                + ChangeEntry.KEY_AUTHOR + " == @" + ChangeEntry.KEY_AUTHOR + " RETURN t", BaseDocument.class,
            ImmutableMap.<String, Object>of(ChangeEntry.KEY_CHANGEID, changeEntry.getChangeId(), ChangeEntry.KEY_AUTHOR, changeEntry.getAuthor()), null
        ).hasNext();
    }

    public void save(ChangeEntry changeEntry) throws ArangobeeConnectionException {
        verifyDbConnection();

        ArangoCollection arangobeeLog = getArangoDatabase().collection(changelogCollectionName);
        arangobeeLog.insertDocument(changeEntry.buildFullDBObject());
    }

    private void verifyDbConnection() throws ArangobeeConnectionException {
        if (getArangoDatabase() == null) {
            throw new ArangobeeConnectionException("Database is not connected. Arangobee has thrown an unexpected error", new NullPointerException());
        }
    }

    private void ensureChangeLogCollectionIndex(ArangoDatabase arangoDatabase, String collectionName) {

        ArangoCollection collection = arangoDatabase.collection(collectionName);
        if (!collection.exists()) {
            CollectionCreateOptions collectionCreateOptions = new CollectionCreateOptions();
            collectionCreateOptions.replicationFactor(2);
            arangoDatabase.createCollection(collectionName, collectionCreateOptions);
        }
        indexDao.createRequiredUniqueIndex(collection);
    }

    public void close() {
        // NO-OP
    }

    private void initializeLock() {
        lockDao.intitializeLock(arangoDatabase);
    }

    public void setIndexDao(ChangeEntryIndexDao changeEntryIndexDao) {
        this.indexDao = changeEntryIndexDao;
    }

    /* Visible for testing */
    void setLockDao(LockDao lockDao) {
        this.lockDao = lockDao;
    }

    public void setChangelogCollectionName(String changelogCollectionName) {
        this.changelogCollectionName = changelogCollectionName;
    }

    public void setLockCollectionName(String lockCollectionName) {
        this.lockDao.setLockCollectionName(lockCollectionName);
    }

    public boolean isWaitForLock() {
        return waitForLock;
    }

    public void setWaitForLock(boolean waitForLock) {
        this.waitForLock = waitForLock;
    }

    public long getChangeLogLockWaitTime() {
        return changeLogLockWaitTime;
    }

    public void setChangeLogLockWaitTime(long changeLogLockWaitTime) {
        this.changeLogLockWaitTime = changeLogLockWaitTime;
    }

    public long getChangeLogLockPollRate() {
        return changeLogLockPollRate;
    }

    public void setChangeLogLockPollRate(long changeLogLockPollRate) {
        this.changeLogLockPollRate = changeLogLockPollRate;
    }

    public boolean isThrowExceptionIfCannotObtainLock() {
        return throwExceptionIfCannotObtainLock;
    }

    public void setThrowExceptionIfCannotObtainLock(boolean throwExceptionIfCannotObtainLock) {
        this.throwExceptionIfCannotObtainLock = throwExceptionIfCannotObtainLock;
    }

}
