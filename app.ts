import { MongoClient, Db, Collection } from "mongodb";
import * as dotenv from "dotenv";
import { faker } from "@faker-js/faker";
import { Customer } from "./common";

dotenv.config();

async function generateCustomers(customersCollection: Collection<Customer>) {
  setInterval(async () => {
    const customersBatch: Customer[] = [];

    const batchSize = Math.floor(Math.random() * 10) + 1;

    for (let i = 0; i < batchSize; i++) {
      const customer: Customer = {
        firstName: faker.person.firstName(),
        lastName: faker.person.lastName(),
        email: faker.internet.email(),
        address: {
          line1: faker.location.streetAddress(),
          line2: faker.location.secondaryAddress(),
          postcode: faker.location.zipCode(),
          city: faker.location.city(),
          state: faker.location.state({ abbreviated: true }),
          country: faker.location.country(),
        },
        createdAt: new Date(),
      };

      customersBatch.push(customer);
    }

    await customersCollection.insertMany(customersBatch);
    console.log(`Inserted ${customersBatch.length} customers`);
  }, 200);
}

async function main() {
  try {
    // Чтение конфигурации из .env файла
    const { DB_URI } = process.env;

    if (!DB_URI) {
      throw new Error("DB_URI is not defined in the .env file");
    }

    // Подключение к MongoDB
    const client = await MongoClient.connect(DB_URI);
    const db = client.db();

    // Получение коллекции customers
    const customersCollection = db.collection<Customer>("customers");

    // Генерация и вставка покупателей
    await generateCustomers(customersCollection);

    console.log("Generating and inserting customers...");
  } catch (error) {
    console.error("An error occurred:", error);
  }
}

main();
