package com.github.arangobee;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.DocumentCreateEntity;
import com.github.arangobee.changeset.ChangeEntry;
import com.github.arangobee.dao.ChangeEntryDao;
import com.github.arangobee.dao.ChangeEntryIndexDao;
import com.github.arangobee.exception.ArangobeeConnectionException;
import com.github.arangobee.exception.ArangobeeLockException;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.core.env.Environment;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


public class AbstractArangobeeTest {
    protected static final String FAKE_LOCK = "fakeLock";

    private static final String CHANGELOG_COLLECTION_NAME = "dbchangelog";
    private final List<BaseDocument> list = new ArrayList<>();
    protected Arangobee runner;
    @Mock
    protected ChangeEntryDao dao;
    @Mock
    private ChangeEntryIndexDao indexDao;
    @Mock
    private ArangoDatabase fakeArangoDatabase;
    @Mock
    private AutowireCapableBeanFactory autowireCapableBeanFactory;
    private Environment environmentMock;

    public void before() throws ArangobeeConnectionException, ArangobeeLockException {
        runner = new Arangobee(fakeArangoDatabase, autowireCapableBeanFactory).dao(dao);
        lenient().when(dao.connectDb(any())).thenReturn(fakeArangoDatabase);
        lenient().when(dao.getArangoDatabase()).thenReturn(fakeArangoDatabase);
        ArangoCollection mockCollection = mock(ArangoCollection.class);
        list.clear();
        lenient().when(mockCollection.insertDocument(any())).then((Answer) invocation -> {
            list.add((BaseDocument) invocation.getArguments()[0]);
            return new DocumentCreateEntity();
        });
        lenient().when(fakeArangoDatabase.collection(anyString())).thenReturn(mockCollection);
        lenient().doAnswer(invocation -> {
            Object o = invocation.getArguments()[0];
            for (Field field : o.getClass().getDeclaredFields()) {
                if (Environment.class.isAssignableFrom(field.getType()) && field.getAnnotation(Autowired.class) != null) {
                    field.setAccessible(true);
                    field.set(o, environmentMock);
                }
            }
            return null;
        }).when(autowireCapableBeanFactory).autowireBeanProperties(any(), anyInt(), anyBoolean());
        lenient().doCallRealMethod().when(dao).save(any(ChangeEntry.class));
        doCallRealMethod().when(dao).setChangelogCollectionName(anyString());
        doCallRealMethod().when(dao).setIndexDao(any(ChangeEntryIndexDao.class));
        dao.setIndexDao(indexDao);
        dao.setChangelogCollectionName(CHANGELOG_COLLECTION_NAME);

//        runner.setEnabled(true);
    }

    protected void setSpringEnvironment(Environment environmentMock) {
        this.environmentMock = environmentMock;
        runner.setSpringEnvironment(environmentMock);
//        autowireCapableBeanFactory.autowireBean(environmentMock);
    }

    protected long count(String changeId, String author) {
        long count = 0;
        for (BaseDocument doc : list) {
            if ((changeId == null || changeId.equals(doc.getAttribute(ChangeEntry.KEY_CHANGEID)))
                && (author == null || author.equals(doc.getAttribute(ChangeEntry.KEY_AUTHOR)))) {
                count++;
            }
        }
        return count;
    }
}
