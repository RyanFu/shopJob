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
 * �������� �汾��
 * </p>
 * <br>
 * ���DO�����޸� ��ע����� �汾�š�
 * <br>
 * java ���л� �Ķ��� ���ǿ��ָ�� serialVersionUID
 * <br>
 * �����ǿ��ָ�� serialVersionUID  ��������ɾ���ֶ�, java ���л������л���̬���ɵ� serialVersionUID �ͻ᲻һ����,����ClassNotFoundException ����
 * <br>
 * java ���л�ʱ�� ������� serialVersionUID ��Ҫƥ�� �����һ����ƥ�� �ͻᱨClassNotFoundException ����
 * <br>
 * ���ǿ��ָ����serialVersionUID ��ֻҪ�� �޸��ֶ����� ���������������Ͳο�javadoc�� �������ֶ� ɾ���ֶ� ���ӷ��� �� ���л��ͷ����л��Ǽ��ݵġ�
 *
 * @author kongwang
 * @version 2008-1-5 ����09:49:39 hu.weih
 */

public class VersionDO implements Serializable {

    /**
     * Comment for <code>serialVersionUID</code>
     */
    private static final long serialVersionUID = 816462269544011168L;

    /**
     * �������������汾
     */
    private static final int UPGRADE = 2;


    /**
     * ��ǰ�汾
     */
    private static final short VERSION = 1;

    /**
     * ��ǰ����İ汾��
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
