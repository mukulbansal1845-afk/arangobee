package com.github.arangobee.dao;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.model.CollectionCreateOptions;


/**
 * @author colsson11
 * @since 13.01.15
 */
public class LockDao {
    //  private static final Logger logger = LoggerFactory.getLogger(LockDao.class);
//    private static final String KEY_PROP_NAME="key";

    //  private static final int INDEX_SORT_ASC = 1;

    private static final String LOCK_ENTRY_KEY_VAL = "LOCK";
    private String lockCollectionName;

    public LockDao(String lockCollectionName) {
        this.lockCollectionName = lockCollectionName;
    }

    public void intitializeLock(ArangoDatabase arangoDatabase) {
        createCollectionAndUniqueIndexIfNotExists(arangoDatabase);
    }

    private void createCollectionAndUniqueIndexIfNotExists(ArangoDatabase arangoDatabase) {
        //	  BaseDocument indexKeys = new BaseDocument();
        //	  indexKeys.addAttribute(KEY_PROP_NAME, INDEX_SORT_ASC);
        //    IndexOptions indexOptions = new IndexOptions().unique(true).name("arangobeelock_key_idx");
        ArangoCollection collection = arangoDatabase.collection(lockCollectionName);
        if (!collection.exists()) {
            CollectionCreateOptions collectionCreateOptions = new CollectionCreateOptions();
            collectionCreateOptions.replicationFactor(2);
            arangoDatabase.createCollection(lockCollectionName, collectionCreateOptions);
        }
        //	  collection.ensurePersistentIndex(ImmutableList.of(KEY_PROP_NAME), new PersistentIndexOptions().unique(true));
    }

    public String acquireLock(ArangoDatabase arangoDatabase) {

        BaseDocument insertObj = new BaseDocument();
        insertObj.setKey(LOCK_ENTRY_KEY_VAL);
        //	  insertObj.addAttribute(KEY_PROP_NAME, LOCK_ENTRY_KEY_VAL);
        insertObj.addAttribute("status", "LOCK_HELD");

        // acquire lock by attempting to insert the same value in the collection - if it already exists (i.e. lock held)
        // there will be an exception
        try {
            return arangoDatabase.collection(lockCollectionName).insertDocument(insertObj).getKey();
        } catch (ArangoDBException ex) {
            return null;
        }
    }

    public void releaseLock(ArangoDatabase arangoDatabase, String lock) {
        // release lock by deleting collection entry
        arangoDatabase.collection(lockCollectionName).deleteDocument(lock);
    }

    /**
     * Check if the lock is held. Could be used by external process for example.
     *
     * @param arangoDatabase {@link ArangoDatabase} object
     * @return true if the lock is currently held
     */
    public boolean isLockHeld(ArangoDatabase arangoDatabase) {
        return arangoDatabase.collection(lockCollectionName).count().getCount() == 1;
    }

    public void setLockCollectionName(String lockCollectionName) {
        this.lockCollectionName = lockCollectionName;
    }

}
