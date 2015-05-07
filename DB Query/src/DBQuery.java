

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class DBQuery {

	public static void main(String[] args) throws Exception {

		// DB Query
				String[] query = {
					
						"SELECT DISTINCT "									+ 
								"ntm_offering.OfferingCode, "                    +
								"ntm_offering.Name AS ProgramOffering_Name, "    +
							                                                    
								"ntm_program.ProgramCode, "                      +
								"ntm_program.Name, "                             +
								"fss_programstatuscode.Name AS Program_Status, " +
								"ntm_program.ProgramStartDate, "                 +
								"ntm_program.ProgramEndDate, "                   +
								"ntm_program.HasApplicationProcess, "            +
								"ntm_program.ApplicationStartDate, "             +
								"ntm_program.ApplicationEndDate, "               +
								"ntm_program.EnrollmentStartDate, "              +
								"ntm_program.EnrollmentEndDate, "				+
								"ntm_certificate.Name AS CertificateName, "      +
							                                                    
								"ntm_programapp.StudentID, "                     +
								"ntm_programapp.StatusID, "                      +
								"ntm_programapp.ApplicationDate, "               +
								                                                
								"ntm_programenrollment.StudentID, "              +
								"ntm_programenrollment.StatusID, "               +
								"ntm_programenrollment.EnrollmentDate, "         +
								                                                
								"ntm_financial.Description,"                    +
								"ntm_financial.ItemUnitAmount " 					+

							"FROM ntm_offering "                                                    +
								"INNER JOIN ntm_program ON "                                        +
								"ntm_offering.OfferingID = ntm_program.OfferingID "                 +

								"INNER JOIN fss_programstatuscode ON "                              +
								"ntm_program.ProgramStatusCodeID = fss_programstatuscode.StatusID " +

								"LEFT OUTER JOIN ntm_certificate ON "                               +
								"ntm_program.CertificateID = ntm_certificate.CertificateID "        +

								"LEFT OUTER JOIN ntm_programapp ON "                                +
								"ntm_program.ProgramID = ntm_programapp.ProgramID "                 +

								"LEFT OUTER JOIN ntm_programenrollment ON "                         +
								"ntm_programapp.ProgramID = ntm_programenrollment.ProgramID "       +

								"LEFT OUTER JOIN ntm_student ON "                                   +
								"ntm_programenrollment.StudentID = ntm_student.StudentID "          +

								"LEFT OUTER JOIN ntm_person ON "                                    +
								"ntm_student.PersonID = ntm_person.PersonID "                       +

								"LEFT OUTER JOIN ntm_programfinancial ON "                          +
								"ntm_program.ProgramID = ntm_programfinancial.ProgramID "           +

								"LEFT OUTER JOIN ntm_financial ON "                                 +
								"ntm_programfinancial.FinancialID = ntm_financial.FinancialID "     +

							"WHERE ntm_program.Name LIKE '%zbt%' "                                  +

							"ORDER BY ntm_program.Name ASC; " ,
												
						// Select all Sections with Finanacial, Optional items & discounts
				/*	"SELECT DISTINCT "                                                                                                                                                                                          +
						"ntm_offering.OfferingCode, "                                                           +
						"ntm_offering.Name as Offering_Name, "                                                  +
						"ntm_offering.HasApprovalProcess, "                                                     +
                                                                                                                
						"ntm_section.SectionNumber, "                                                           +
						"fss_sectionstatuscode.Name as Section_Status, "										+
						"ntm_section.FinalEnrollmentDate, "                                                     +
                                                                                                                
						"ntm_seatgroup.Name as SeatGroup_Name, "                                                +
						"ntm_seatgroup.NumberOfSeats, "                                                         +
						"ntm_seatgroup.IsDefault, "                                                             +
                                                                                                                
						"ntm_sectionfinancial.Description as SectionFinancial_Name, "                           +
						"ntm_sectionfinancial.IsOptional, "                                                     +
						"ntm_sectionfinancial.IsActive, "                                                       +
						"ref_glaccount.Name as GL_Name, "                                                       +
                                                                                                                
						"ntm_discountprogram.ShortName as DiscountProgram_Name, "                               +
						"ref_discounttype.Name as DiscountType, "                                               +
                                                                                                                
						"ntm_discountprogram.Amount as Discount_Amount, "                                       +
						"ref_discountamounttype.Name as Amount_Unit, "                                          +
                                                                                                                
						"ntm_discountprogram.IsPromotedForMarketing, "                                          +
						"ntm_discountprogram.IsActive "                                                         +
                                                                                                                
					"FROM ntm_offering "                                                                        +
						"INNER JOIN ntm_section on "                                                            +
						"ntm_offering.OfferingID = ntm_section.OfferingID "                                     +
						                                         	                                            
						"INNER JOIN ntm_seatgroup on "                                                          +
						"ntm_section.SectionID = ntm_seatgroup.SectionID "                                      +
						
						"INNER JOIN fss_sectionstatuscode on "                                                  +
						"ntm_section.SectionStatusCodeID = fss_sectionstatuscode.StatusID "                    +
                                                                                                                
						"LEFT OUTER JOIN ntm_seatgroupfinancial on "                                            +
						"ntm_seatgroup.SeatGroupID = ntm_seatgroupfinancial.SeatGroupID "                       +
                                                                                                               
						"LEFT OUTER JOIN ntm_sectionfinancial on "                                              +
						"ntm_seatgroupfinancial.SectionFinancialID = ntm_sectionfinancial.SectionFinancialID "  +
                                                                                                                
						"LEFT OUTER JOIN ref_glaccount on "                                                     +
						"ref_glaccount.ID = ntm_sectionfinancial.GLAccountID "                                  +
                                                                                                                
						"LEFT OUTER JOIN ntm_sectiondiscount on "                                               +
						"ntm_sectionfinancial.SectionFinancialID = ntm_sectiondiscount.SectionFinancialID "     +
                                                                                                               
						"LEFT OUTER JOIN ntm_discountprogram on "                                               +
						"ntm_sectiondiscount.DiscountProgramID = ntm_discountprogram.DiscountProgramID "        +
                                                                                                               
						"LEFT OUTER JOIN ref_discounttype on "                                                  +
						"ref_discounttype.ID = ntm_discountprogram.DiscountTypeID "                             +
                                                                                                                
						"LEFT OUTER JOIN ref_discountamounttype on "                                            +
						"ntm_discountprogram.AmountTypeID = ref_discountamounttype.ID "                         +
                                                                                                               
					"WHERE ntm_section.SectionNumber like '%1112-%' "                                              +

					"ORDER BY ntm_section.SectionNumber ASC;",	*/	
					
				// Select all Students with Purchaser role 
				"SELECT DISTINCT "                             +
					"ntm_student.StudentID, "                  +         
					"ntm_person.FirstName, "                   +         
					"ntm_person.MiddleName, "                  +
					"ntm_person.LastName, "                    +
					"ntm_person.Birthday, "                    +
					"ntm_person.GovID, "                       +
					"ntm_personaddress.AddressLine1, "         +
					"ntm_personaddress.AddressLine2, "         +
					"ntm_personaddress.AddressLine3, "         +
					"ntm_personaddress.Locality as City, "     +     
					"ntm_personaddress.PostalCode, "           +
					"ref_countrycode.Description as Country, " +        
					"ntm_persontelephone.TelephoneNumber, "    +
					"ntm_personemail.EmailAddress, "           +
					"ntm_personlogin.UserLogin as LoginName "  +

				"FROM ntm_student "	                                         	 +
					"INNER JOIN ntm_person on "                                  +
					"ntm_student.PersonID = ntm_person.PersonID "	             +
					"LEFT OUTER JOIN ntm_personlogin on "                        +
					"ntm_person.PersonID = ntm_personlogin.PersonID "            +
					"LEFT OUTER JOIN ntm_personaddress on "                      +
					"ntm_person.PersonID = ntm_personaddress.PersonID "          +
					"LEFT OUTER JOIN ref_countrycode on "                        +
					"ntm_personaddress.CountryCodeID = ref_countrycode.ID "		 +
					"LEFT OUTER JOIN ntm_persontelephone on "                    +
					"ntm_personaddress.PersonID = ntm_persontelephone.PersonID " +
					"LEFT OUTER JOIN ntm_personemail on "                        +
					"ntm_persontelephone.PersonID = ntm_personemail.PersonID "	 +

				"WHERE ntm_personaddress.AddressTypeID = '4' "		 	 +
					"AND ntm_personemail.EmailAddressTypeID = '1' "		 +
					"AND ntm_persontelephone.TelephoneTypeID = '1' "	 +
					"AND ntm_personlogin.UserLogin IS NOT NULL "		 +
					"AND ntm_personemail.EmailAddress LIKE '%jcarter%' " +
				"ORDER BY ntm_student.StudentID ASC;",
				
				// Select all Instructors with Purchaser role
				"SELECT DISTINCT "                             +
					"ntm_faculty.FacultyID as InstructorID, "  +         
					"ntm_person.FirstName, "                   +         
					"ntm_person.MiddleName, "                  +
					"ntm_person.LastName, "                    +
					"ntm_person.Birthday, "                    +
					"ntm_person.GovID, "                       +
					"ntm_personaddress.AddressLine1, "         +
					"ntm_personaddress.AddressLine2, "         +
					"ntm_personaddress.AddressLine3, "         +
					"ntm_personaddress.Locality as City, "     +     
					"ntm_personaddress.PostalCode, "           +
					"ref_countrycode.Description as Country, " +        
					"ntm_persontelephone.TelephoneNumber, "    +
					"ntm_personemail.EmailAddress, "           +
					"ntm_personlogin.UserLogin as LoginName "  +

				"FROM ntm_faculty "	                                         	 +
					"INNER JOIN ntm_person on "                                  +
					"ntm_faculty.PersonID = ntm_person.PersonID "	             +
					"LEFT OUTER JOIN ntm_personlogin on "                        +
					"ntm_person.PersonID = ntm_personlogin.PersonID "            +
					"LEFT OUTER JOIN ntm_personaddress on "                      +
					"ntm_person.PersonID = ntm_personaddress.PersonID "          +
					"LEFT OUTER JOIN ref_countrycode on "                        +
					"ntm_personaddress.CountryCodeID = ref_countrycode.ID "		 +
					"LEFT OUTER JOIN ntm_persontelephone on "                    +
					"ntm_personaddress.PersonID = ntm_persontelephone.PersonID " +
					"LEFT OUTER JOIN ntm_personemail on "                        +
					"ntm_persontelephone.PersonID = ntm_personemail.PersonID "	 +

				"WHERE ntm_personaddress.AddressTypeID = '4' "		 	 +
					"AND ntm_personemail.EmailAddressTypeID = '1' "		 +
					"AND ntm_persontelephone.TelephoneTypeID = '1' "	 +
					"AND ntm_personlogin.UserLogin IS NOT NULL "		 +
				//	"AND ntm_personemail.EmailAddress LIKE '%jcarter%' " +
					"ORDER BY ntm_faculty.FacultyID ASC;",

						"SELECT PersonID, FirstName, MiddleName, LastName FROM ntm_person WHERE LastName LIKE '%dff%';",
						"SELECT PersonID, FirstName, MiddleName, LastName, Birthday FROM ntm_person WHERE Birthday IS NULL;",
						"SELECT * FROM ref_glaccount;"
				};
		
	//	processQuery("dev-d0003", "hir_db", "mysql", query[0]);
	//	processQuery("dev-d0003", "hir_db", "mysql", query[0]);
	//	processQuery("test-d0003", "hir_sample_customer", "mysql", query[0]);
		
	//	processQuery("10.0.0.2", "hir_db", "mysql", query[0]);
	//	processQuery("localhost", "jxntm_gatewaygl", "mysql", query[0]);
	//	processQuery("devinstance3", "jxntm4_2_49", "mysql", query[0]);
		processQuery("devinstance3", "jxntm4_2_49", "mysql", query[0]);

	}

	public static void processQuery(String host, String dbName, String dbType, String query) throws Exception {
		
		Connection con = new DBSelect().getDBConnection(host, dbName, dbType);

		// Create statement object
		Statement stmt = con.createStatement();

		// Send SQL query to DB and saving returned result
		ResultSet rs = stmt.executeQuery(query);
		System.out.println("\nSelected host: " + host + "\nSelected DB: " + dbName + "\nDB Type: " + dbType + "\n");
		showResult(rs);

		rs.close();
		stmt.close();
		con.close();
	}

	public static void showResult(ResultSet rs) throws SQLException {

		// To extract data from Resultset object
		if (!rs.next()) {
			System.out.println("No records found");

		} else {
			rs.beforeFirst(); // record pointer is placed before first row		
			ResultSetMetaData metaData = rs.getMetaData();
						
			int colCounter = metaData.getColumnCount();
			int[] colspan = new int[colCounter+1];

			for(int i=1; i<=colCounter; i++) {
				colspan[i] = metaData.getColumnLabel(i).length();
			}

			for(int i = 1; i <= colCounter; i++) {
				while(rs.next()) {
					if ( (rs.getString(i) != null) && !(metaData.getColumnTypeName(i).equalsIgnoreCase("DATETIME")) ) {					
						if((rs.getString(i).length() >= colspan[i])) {
							colspan[i] = rs.getString(i).length();
						}
					}
					
					if (metaData.getColumnTypeName(i).equalsIgnoreCase("DATETIME")) {
						if((rs.getDate(i).toString().length() >= colspan[i])) {
							colspan[i] = rs.getDate(i).toString().length();
						}						
					}

				}
				rs.beforeFirst(); // record pointer is placed before first row	

			}
			
			// Printing separators
						System.out.print("+=");
						for (int i = 1; i <= colCounter; i++) {
							for (int j = 1; j <= colspan[i]; j++) {
								System.out.print("=");
							}
							if (i < colCounter) {
								System.out.print("=+=");
							} else {
								System.out.print("=+");
							}
							
						}
						System.out.println();
			

			// Printing Column names dynamically according to ResultSetMetaData
						System.out.print("| ");
						for (int i = 1; i <= colCounter; i++) {
							System.out.printf("%-" + colspan[i] + "s | ", metaData.getColumnLabel(i));
						}
						System.out.println();
						
						System.out.print("| ");
						for (int i = 1; i <= colCounter; i++) {
							System.out.printf("%-" + colspan[i] + "s | ", metaData.getColumnTypeName(i));
						}
						System.out.println();

						// Printing separators
						System.out.print("+=");
						for (int i = 1; i <= colCounter; i++) {
							for (int j = 1; j <= colspan[i]; j++) {
								System.out.print("=");
							}
							if (i < colCounter) {
								System.out.print("=+=");
							} else {
								System.out.print("=+");
							}
						}
						System.out.println();

			rs.beforeFirst(); // record pointer is placed before first row
			// Printing values according to ResultSetMetaData
			while (rs.next()) {
				System.out.print("| ");
				for (int i = 1; i <= colCounter; i++) {
					if ( metaData.getColumnTypeName(i).equalsIgnoreCase("DATETIME") ) {
						System.out.printf("%-" + colspan[i] + "s | ", rs.getDate(i));
					} else if ( metaData.getColumnTypeName(i).equalsIgnoreCase("TINYINT") ) {
						System.out.printf("%-" + colspan[i] + "s | ", rs.getBoolean(i));
					} else {
						System.out.printf("%-" + colspan[i] + "s | ", rs.getString(i));
					}	
				}
				System.out.println();
				
				System.out.print("+-");
				for (int i = 1; i <= colCounter; i++) {
					for (int j = 1; j <= colspan[i]; j++) {
						System.out.print("-");
					}
					if (i < colCounter) {
						System.out.print("-+-");
					} else {
						System.out.print("-+");
					}

				}
				System.out.println();
			}

			rs.last(); // record pointer is placed on the last row
			System.out.println("\n\n\tNumber of record(s) found: " + rs.getRow());
			System.out.println();
		}
	}
}
