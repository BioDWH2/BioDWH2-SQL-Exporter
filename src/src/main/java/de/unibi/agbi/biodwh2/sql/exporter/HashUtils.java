package de.unibi.agbi.biodwh2.sql.exporter;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;

final class HashUtils {
    private HashUtils() {
    }

    static String getFastPseudoHashFromFile(final String filePath) throws IOException {
        final BasicFileAttributes attributes = Files.readAttributes(Paths.get(filePath), BasicFileAttributes.class);
        return DigestUtils.md5Hex(attributes.lastModifiedTime() + "__" + attributes.size());
    }
}
