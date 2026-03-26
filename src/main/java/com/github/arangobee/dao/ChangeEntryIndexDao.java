package com.github.arangobee.dao;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.model.PersistentIndexOptions;
import com.github.arangobee.changeset.ChangeEntry;
import com.google.common.collect.ImmutableList;

/**
 * @author lstolowski
 * @since 10.12.14
 */
public class ChangeEntryIndexDao {
    public ChangeEntryIndexDao(String changelogCollectionName) {
        //		this.changelogCollectionName = changelogCollectionName;
    }

    public void createRequiredUniqueIndex(ArangoCollection collection) {
        collection.ensurePersistentIndex(ImmutableList.of(ChangeEntry.KEY_CHANGEID, ChangeEntry.KEY_AUTHOR), new PersistentIndexOptions().unique(true));
    }

    //	public BaseDocument findRequiredChangeAndAuthorIndex(ArangoDatabase db) {

    /// /		ArangoCollection indexes = db.collection("system.indexes");
    /// /		BaseDocument doc=new BaseDocument();
    /// /		doc.addAttribute("ns", db.name() + "." + changelogCollectionName);
    /// /		BaseDocument subDoc = new BaseDocument();
    /// /		subDoc.addAttribute(ChangeEntry.KEY_CHANGEID, 1);
    /// /		subDoc.addAttribute(ChangeEntry.KEY_AUTHOR, 1);
    /// /		doc.addAttribute("key", subDoc);
    /// /		BaseDocument index = indexes.find(subDoc
    /// /				.append("ns", db.name() + "." + changelogCollectionName)
    /// /				.append("key", new Document().append(ChangeEntry.KEY_CHANGEID, 1).append(ChangeEntry.KEY_AUTHOR, 1))
    /// /				).first();
    //
    //		return db.query(
    //				"FOR t IN system.indexes FILTER t.ns == @name && t.key." + ChangeEntry.KEY_CHANGEID + "==1 && t.key."
    //						+ ChangeEntry.KEY_AUTHOR + "==1 RETURN t",
    //				ImmutableMap.<String, Object>of("name", /*db.name() + "." +*/ changelogCollectionName), null,
    //				BaseDocument.class).next();
    //	}
    public boolean isUnique(BaseDocument index) {
        Object unique = index.getAttribute("unique");
        if (unique != null && unique instanceof Boolean) {
            return (Boolean) unique;
        } else {
            return false;
        }
    }

    public void dropIndex(ArangoDatabase db, BaseDocument index) {
        db.deleteIndex(index.getAttribute("name").toString());
    }

    //	public void setChangelogCollectionName(String changelogCollectionName) {
    //		this.changelogCollectionName = changelogCollectionName;
    //	}

}
