package com.github.arangobee;

import com.arangodb.ArangoDatabase;
import com.arangodb.springframework.core.template.ArangoTemplate;
import com.github.arangobee.changeset.ChangeEntry;
import com.github.arangobee.dao.ChangeEntryDao;
import com.github.arangobee.exception.ArangobeeChangeSetException;
import com.github.arangobee.exception.ArangobeeConfigurationException;
import com.github.arangobee.exception.ArangobeeConnectionException;
import com.github.arangobee.exception.ArangobeeException;
import com.github.arangobee.utils.ChangeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.core.env.Environment;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static org.springframework.util.StringUtils.hasText;

/**
 * Arangobee runner
 *
 * @author lstolowski
 * @since 26/07/2014
 */
public class Arangobee implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(Arangobee.class);

    private static final String DEFAULT_CHANGELOG_COLLECTION_NAME = "dbchangelog";
    private static final String DEFAULT_LOCK_COLLECTION_NAME = "arangolock";
    private static final boolean DEFAULT_WAIT_FOR_LOCK = false;
    private static final long DEFAULT_CHANGE_LOG_LOCK_WAIT_TIME = 5L;
    private static final long DEFAULT_CHANGE_LOG_LOCK_POLL_RATE = 10L;
    private static final boolean DEFAULT_THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK = false;
    private final ArangoDatabase arangoDatabase;
    private final AutowireCapableBeanFactory autowireCapableBeanFactory;
    private final int autowireMode;
    private ChangeEntryDao dao;
    //    private boolean enabled=true;
    private String changeLogsScanPackage;
    private Environment springEnvironment;

    public Arangobee(ArangoDatabase arangoDatabase, AutowireCapableBeanFactory autowireCapableBeanFactory) {
        this(arangoDatabase, autowireCapableBeanFactory, AutowireCapableBeanFactory.AUTOWIRE_NO);
    }

    public Arangobee(ArangoDatabase arangoDatabase, AutowireCapableBeanFactory autowireCapableBeanFactory, int autowireMode) {
        this.arangoDatabase = arangoDatabase;
        this.autowireCapableBeanFactory = autowireCapableBeanFactory;
        this.autowireMode = autowireMode;
        this.dao = new ChangeEntryDao(DEFAULT_CHANGELOG_COLLECTION_NAME, DEFAULT_LOCK_COLLECTION_NAME, DEFAULT_WAIT_FOR_LOCK, DEFAULT_CHANGE_LOG_LOCK_WAIT_TIME,
            DEFAULT_CHANGE_LOG_LOCK_POLL_RATE, DEFAULT_THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK);
    }

    public Arangobee dao(ChangeEntryDao dao) {
        this.dao = dao;
        return this;
    }

    /**
     * For Spring users: executing arangobee after bean is created in the Spring context
     *
     * @throws Exception exception
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        execute();
    }

    /**
     * Executing migration
     *
     * @throws ArangobeeException exception
     */
    public void execute() throws ArangobeeException {
//        if (!isEnabled()) {
//            logger.info("Arangobee is disabled. Exiting.");
//            return;
//        }

        validateConfig();

        dao.connectDb(this.arangoDatabase);

        String lock = dao.acquireProcessLock();
        if (lock == null) {
            throw new ArangobeeException("Arangobee did not acquire process lock. Exiting.");
        }

        logger.info("Arangobee acquired process lock, starting the data migration sequence..");

        try {
            executeMigration();
        } finally {
            logger.info("Arangobee is releasing process lock.");
            dao.releaseProcessLock(lock);
        }

        logger.info("Arangobee has finished his job.");
    }

    private void executeMigration() throws ArangobeeConnectionException, ArangobeeException {

        ChangeService service = new ChangeService(changeLogsScanPackage, springEnvironment);

        for (Class<?> changelogClass : service.fetchChangeLogs()) {

            Object changelogInstance = null;
            try {
                changelogInstance = changelogClass.getConstructor().newInstance();
                if (autowireCapableBeanFactory != null)
                    autowireCapableBeanFactory.autowireBeanProperties(changelogInstance, autowireMode, true);
                List<Method> changesetMethods = service.fetchChangeSets(changelogInstance.getClass());

                for (Method changesetMethod : changesetMethods) {
                    ChangeEntry changeEntry = service.createChangeEntry(changesetMethod);

                    try {
                        if (dao.isNewChange(changeEntry)) {
                            executeChangeSetMethod(changesetMethod, changelogInstance, dao.getArangoDatabase());
                            dao.save(changeEntry);
                            logger.info(changeEntry + " applied");
                        } else if (service.isRunAlwaysChangeSet(changesetMethod)) {
                            executeChangeSetMethod(changesetMethod, changelogInstance, dao.getArangoDatabase());
                            logger.info(changeEntry + " reapplied");
                        } else {
                            logger.info(changeEntry + " passed over");
                        }
                    } catch (ArangobeeChangeSetException e) {
                        logger.error(e.getMessage());
                    }
                }
            } catch (NoSuchMethodException e) {
                throw new ArangobeeException(e.getMessage(), e);
            } catch (IllegalAccessException e) {
                throw new ArangobeeException(e.getMessage(), e);
            } catch (InvocationTargetException e) {
                Throwable targetException = e.getTargetException();
                throw new ArangobeeException(targetException.getMessage(), e);
            } catch (InstantiationException e) {
                throw new ArangobeeException(e.getMessage(), e);
            }

        }
    }

    private Object executeChangeSetMethod(Method changeSetMethod, Object changeLogInstance, ArangoDatabase arangoDatabase)
        throws IllegalAccessException, InvocationTargetException, ArangobeeChangeSetException {
        if (changeSetMethod.getParameterTypes().length == 2 && changeSetMethod.getParameterTypes()[0].equals(ArangoTemplate.class)
            && changeSetMethod.getParameterTypes()[1].equals(Environment.class)) {
            logger.debug("method with ArangoTemplate and environment arguments");

            return changeSetMethod.invoke(changeLogInstance, arangoDatabase, springEnvironment);
        } else if (changeSetMethod.getParameterTypes().length == 1 && changeSetMethod.getParameterTypes()[0].equals(ArangoDatabase.class)) {
            logger.debug("method with DB argument");

            return changeSetMethod.invoke(changeLogInstance, arangoDatabase);
        } else if (changeSetMethod.getParameterTypes().length == 0) {
            logger.debug("method with no params");

            return changeSetMethod.invoke(changeLogInstance);
        } else {
            throw new ArangobeeChangeSetException(
                "ChangeSet method " + changeSetMethod.getName() + " has wrong arguments list. Please see docs for more info!");
        }
    }

    private void validateConfig() throws ArangobeeConfigurationException {
        if (arangoDatabase == null) {
            throw new ArangobeeConfigurationException("Database is not set.");
        }
        if (!hasText(changeLogsScanPackage)) {
            throw new ArangobeeConfigurationException("Scan package for changelogs is not set: use appropriate setter");
        }
    }

    /**
     * @return true if an execution is in progress, in any process.
     * @throws ArangobeeConnectionException exception
     */
    public boolean isExecutionInProgress() throws ArangobeeConnectionException {
        return dao.isProccessLockHeld();
    }

    /**
     * Package name where @ChangeLog-annotated classes are kept.
     *
     * @param changeLogsScanPackage package where your changelogs are
     * @return Arangobee object for fluent interface
     */
    public Arangobee setChangeLogsScanPackage(String changeLogsScanPackage) {
        this.changeLogsScanPackage = changeLogsScanPackage;
        return this;
    }

//    /**
//     * @return true if Arangobee runner is enabled and able to run, otherwise false
//     */
//    public boolean isEnabled() {
//        return enabled;
//    }
//
//    /**
//     * Feature which enables/disables Arangobee runner execution
//     *
//     * @param enabled Arangobee will run only if this option is set to true
//     * @return Arangobee object for fluent interface
//     */
//    public Arangobee setEnabled(boolean enabled) {
//        this.enabled=enabled;
//        return this;
//    }

    /**
     * Feature which enables/disables waiting for lock if it's already obtained
     *
     * @param waitForLock Arangobee will be waiting for lock if it's already obtained if this option is set to true
     * @return Arangobee object for fluent interface
     */
    public Arangobee setWaitForLock(boolean waitForLock) {
        this.dao.setWaitForLock(waitForLock);
        return this;
    }

    /**
     * Waiting time for acquiring lock if waitForLock is true
     *
     * @param changeLogLockWaitTime Waiting time in minutes for acquiring lock
     * @return Arangobee object for fluent interface
     */
    public Arangobee setChangeLogLockWaitTime(long changeLogLockWaitTime) {
        this.dao.setChangeLogLockWaitTime(changeLogLockWaitTime);
        return this;
    }

    /**
     * Poll rate for acquiring lock if waitForLock is true
     *
     * @param changeLogLockPollRate Poll rate in seconds for acquiring lock
     * @return Arangobee object for fluent interface
     */
    public Arangobee setChangeLogLockPollRate(long changeLogLockPollRate) {
        this.dao.setChangeLogLockPollRate(changeLogLockPollRate);
        return this;
    }

    /**
     * Feature which enables/disables throwing ArangobeeLockException if Arangobee can not obtain lock
     *
     * @param throwExceptionIfCannotObtainLock Arangobee will throw ArangobeeLockException if lock can not be obtained
     * @return Arangobee object for fluent interface
     */
    public Arangobee setThrowExceptionIfCannotObtainLock(boolean throwExceptionIfCannotObtainLock) {
        this.dao.setThrowExceptionIfCannotObtainLock(throwExceptionIfCannotObtainLock);
        return this;
    }

    /**
     * Set Environment object for Spring Profiles (@Profile) integration
     *
     * @param environment org.springframework.core.env.Environment object to inject
     * @return Arangobee object for fluent interface
     */
    public Arangobee setSpringEnvironment(Environment environment) {
        this.springEnvironment = environment;
        return this;
    }

    /**
     * Overwrites a default Arangobee changelog collection hardcoded in DEFAULT_CHANGELOG_COLLECTION_NAME.
     * <p>
     * CAUTION! Use this method carefully - when changing the name on a existing system,
     * your changelogs will be executed again on your ArangoDB instance
     *
     * @param changelogCollectionName a new changelog collection name
     * @return Arangobee object for fluent interface
     */
    public Arangobee setChangelogCollectionName(String changelogCollectionName) {
        this.dao.setChangelogCollectionName(changelogCollectionName);
        return this;
    }

    /**
     * Overwrites a default Arangobee lock collection hardcoded in DEFAULT_LOCK_COLLECTION_NAME
     *
     * @param lockCollectionName a new lock collection name
     * @return Arangobee object for fluent interface
     */
    public Arangobee setLockCollectionName(String lockCollectionName) {
        this.dao.setLockCollectionName(lockCollectionName);
        return this;
    }

    /**
     * Closes the Arango instance used by Arangobee.
     * This will close either the connection Arangobee was initiated with or that which was internally created.
     */
    public void close() {
        dao.close();
    }
}
