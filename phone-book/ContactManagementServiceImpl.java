import java.util.HashMap;
import java.util.Map;

public class ContactManagementServiceImpl implements ContactService {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		final String searchKey = "9176185370";
		final String editKey = "9176185370";
		ContactManagementServiceImpl object = new ContactManagementServiceImpl();
		Map<String, ContactVO> contactMap = new HashMap<String, ContactVO>();
		ContactVO contactVO = new ContactVO();
		contactVO.setContactNumber("9176185370");
		contactVO.setName("Amit Agarwal");
		contactVO.setEmailId("agarwalamit272@gmail.com");

		// Calling add Contact Function.
		object.addContact(contactMap, contactVO);

		// Calling search Contact Function.
		object.searchContact(contactMap, searchKey);

		// Calling edit Contact Function.
		if (object.editContact(contactMap, editKey) != null) {
			object.addContact(contactMap,
					object.editContact(contactMap, editKey));
			object.searchContact(contactMap, searchKey);
		} else {
			System.out.println("Contact doesn't exists.");
		}

	}

	@Override
	public void addContact(Map<String, ContactVO> obj, ContactVO contactVO) {
		if (obj.get(contactVO.getContactNumber()) == null) {
			obj.put(contactVO.getContactNumber(), contactVO);
			System.out.println("Contact Added Successfully !!\n");
		}

		else {
			obj.put(contactVO.getContactNumber(), contactVO);
			System.out.println("Contact Edited Successfully !!\n");
		}
	}

	@Override
	public ContactVO editContact(Map<String, ContactVO> obj, String editKey) {
		if (obj.get(editKey) == null) {
			System.out.println("Contact doesn't present !!");
		} else {
			ContactVO contactVO = obj.get(editKey);
			contactVO.setEmailId("ami_agrl@yahoo.com");
			return contactVO;
		}
		return null;
	}

	@Override
	public void searchContact(Map<String, ContactVO> obj, String searchKey) {
		if (obj.get(searchKey) == null) {
			System.out.println("No such contacts exists !!!");
		} else {
			ContactVO contactVO = obj.get(searchKey);
			System.out.println("Mobile Number:" + contactVO.getContactNumber()
					+ "\n" + "Name:" + contactVO.getName() + "\n" + "Email:"
					+ contactVO.getEmailId()+"\n");
		}

	}

}
