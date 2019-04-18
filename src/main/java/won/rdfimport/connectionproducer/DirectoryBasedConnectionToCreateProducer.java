/*
 * Copyright 2012  Research Studios Austria Forschungsges.m.b.H.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package won.rdfimport.connectionproducer;

import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * AtomProducer that is configured to read atoms from a directory.
 */
public class DirectoryBasedConnectionToCreateProducer implements ConnectionToCreateProducer {
    private static final int NOT_INITIALIZED = -1;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private File directory;

    // Java Regex for filtering filenames in the directory
    private String filenameFilterRegex = null;

    private File[] files;

    private AtomicBoolean initialized = new AtomicBoolean(false);

    private ConnectionToCreateFromRdfProducer fromRdfProducer = new ConnectionToCreateFromRdfProducer();

    @Override
    public Iterator<ConnectionToCreate> getConnectionIterator() {
        initializeLazily();
        return new DirectoryBasedConnectionToCreateIterator();
    }

    public class DirectoryBasedConnectionToCreateIterator implements Iterator<ConnectionToCreate> {
        private volatile int fileIndex = NOT_INITIALIZED;

        public DirectoryBasedConnectionToCreateIterator() {
            this.fileIndex = 0;
        }

        @Override
        public boolean hasNext() {
            return files != null && this.fileIndex >= 0 && this.fileIndex < files.length;
        }

        @Override
        public ConnectionToCreate next() {
            return create();
        }


        private ConnectionToCreate create() {
            int fileIndexToUse = NOT_INITIALIZED;
            synchronized (this) {
                if (!hasNext()) {
                    return null;
                }

                //loop until we find a readable file
                while (this.fileIndex < files.length) {
                    if (isCurrentFileReadable()) {
                        fileIndexToUse = this.fileIndex; //remember the current index
                        break;
                    }
                    this.fileIndex++;
                }
                //fileIndex is now the index we'll use, or it is files.length -1
                //advance the index for next time, which will exhaust the iterator if we're at position files.length-1
                this.fileIndex++;
            }
            if (fileIndexToUse != NOT_INITIALIZED) {
                return makeConnectionToCreate(fileIndexToUse);
            }
            return null;
        }

        public String getCurrentFileName() {
            return files[fileIndex].getName();
        }

        private boolean isCurrentFileReadable() {
            return files[this.fileIndex].isFile() && files[this.fileIndex].canRead();
        }

        private ConnectionToCreate makeConnectionToCreate(final int fileIndexToUse) {
            Model model = readModel(fileIndexToUse);
            //find the 'own atom'
            return fromRdfProducer.makeConnectionToCreate(model);
        }

        private Model readModel(int fileIndexToUse) {
            try {
                //make a ConnectionToCreate object from the file
                //the file must be an RDF file. read it.
                Model model = ModelFactory.createDefaultModel();
                RDFDataMgr.read(model, files[fileIndexToUse].getAbsolutePath());
                return model;
            } catch (Exception e) {
                logger.debug("could not read atom from file {}", files[fileIndexToUse]);
            }
            return null;
        }

    }

    private synchronized void initializeLazily() {
        if (!initialized.get()) {
            init();
        }
    }

    private synchronized void init() {
        if (this.initialized.get()) return;
        if (this.directory == null) {
            logger.warn("No directory specified for DirectoryBasedConnectionToCreateProducer, not reading any data.");
            return;
        }
        logger.debug("Initializing DirectoryBasedConnectionToCreateProducer from directory {}", this.directory);
        this.files = directory.listFiles(createFileFilter());
        if (this.files == null || this.files.length == 0) {
            logger.info("no files found in directory {} with regex {}", this.directory, this.filenameFilterRegex);
        } else {
            logger.debug("found {} files in directory {} with regex {}", new Object[]{files.length, this.directory, this.filenameFilterRegex});
        }
        this.initialized.set(true);
    }

    private FileFilter createFileFilter() {
        if (this.filenameFilterRegex == null) return TrueFileFilter.TRUE;
        return new RegexFileFilter(this.filenameFilterRegex);
    }

    public File getDirectory() {
        return directory;
    }

    public void setDirectory(final File directory) {
        this.directory = directory;
    }

    public String getFilenameFilterRegex() {
        return filenameFilterRegex;
    }

    public void setFilenameFilterRegex(final String filenameFilterRegex) {
        this.filenameFilterRegex = filenameFilterRegex;
    }
}
