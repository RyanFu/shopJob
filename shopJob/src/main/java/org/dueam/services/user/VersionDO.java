//===================================================================
// Created on 2008-1-5
//===================================================================
package org.dueam.services.user;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * <p>
 * 用来控制 版本。
 * </p>
 * <br>
 * 如果DO有做修改 请注意更新 版本号。
 * <br>
 * java 序列化 的对象 最好强制指定 serialVersionUID
 * <br>
 * 如果不强制指定 serialVersionUID  对象增加删除字段, java 序列化反序列化动态生成的 serialVersionUID 就会不一样了,导致ClassNotFoundException 错误。
 * <br>
 * java 序列化时候 所有类的 serialVersionUID 都要匹配 如果有一个不匹配 就会报ClassNotFoundException 错误。
 * <br>
 * 如果强制指定了serialVersionUID 你只要不 修改字段类型 （其他不兼容类型参考javadoc） ，增加字段 删除字段 增加方法 等 序列化和反序列化是兼容的。
 *
 * @author kongwang
 * @version 2008-1-5 上午09:49:39 hu.weih
 */

public class VersionDO implements Serializable {

    /**
     * Comment for <code>serialVersionUID</code>
     */
    private static final long serialVersionUID = 816462269544011168L;

    /**
     * 允许升级三个版本
     */
    private static final int UPGRADE = 2;


    /**
     * 当前版本
     */
    private static final short VERSION = 1;

    /**
     * 当前对象的版本号
     */
    private Long versionObject;

    private void writeObject(ObjectOutputStream stream)
            throws IOException {
        stream.writeShort(VERSION);
    }

    private void readObject(ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
        short v = stream.readShort();
        if (versionConflict(v))
            throw new ClassNotFoundException("local class incompatible: stream classdesc Version = " + v + " local class Version = " + VERSION + " upgrade num is " + UPGRADE);
    }

    /**
     * @param v
     * @return
     */
    private boolean versionConflict(short v) {
        if (Math.abs((v - VERSION)) > UPGRADE) return true;
        return false;
    }

    public Long getVersionObject() {
        return versionObject;
    }

    public void setVersionObject(Long versionObject) {
        this.versionObject = versionObject;
    }

}
