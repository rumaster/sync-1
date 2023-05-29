import { MongoClient, Db, Collection } from "mongodb";
import * as dotenv from "dotenv";
import { Customer } from "./common";

dotenv.config();

// Функция для генерации случайной строки
function generateRandomString(length: number = 8): string {
  const characters =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  let randomString = "";

  for (let i = 0; i < length; i++) {
    const randomIndex = Math.floor(Math.random() * characters.length);
    randomString += characters.charAt(randomIndex);
  }

  return randomString;
}
const convertDocument = (customer: Customer): Customer => {
  customer.firstName = generateRandomString();
  customer.lastName = generateRandomString();
  customer.email = customer.email.replace(/^[^@]+/, generateRandomString());
  customer.address.line1 = generateRandomString();
  customer.address.line2 = generateRandomString();
  customer.address.postcode = generateRandomString();

  return customer;
};

async function synchronizeDocuments(
  customersCollection: Collection<Customer>,
  customersAnonCollection: Collection<Customer>
): Promise<void> {
  const pipeline = [
    {
      $lookup: {
        from: 'customers_anonymised',
        localField: '_id',
        foreignField: '_id',
        as: 'existingDocuments'
      }
    },
    {
      $match: {
        existingDocuments: { $size: 0 }
      }
    },
  ];

  const newDocumentsCursor = await customersCollection.aggregate(pipeline);

  let batchDocuments = [];

  while (await newDocumentsCursor.hasNext()) {
    // Копирование нового документа в коллекцию customers_anonymised  с модификацией полей
    const document = convertDocument(await newDocumentsCursor.next() as Customer);

    batchDocuments.push(document);

    if (batchDocuments.length === 1000) {
      // Добавление пачки синхронизированных документов в коллекцию customers_anonymised
      await customersAnonCollection.insertMany(batchDocuments);

      console.log(`Synchronized ${batchDocuments.length} documents from customers to customers_anonymised`);

      batchDocuments = [];
    }
  }

  // Добавление оставшихся документов, если есть
  if (batchDocuments.length > 0) {
    await customersAnonCollection.insertMany(batchDocuments);
    console.log(`Synchronized ${batchDocuments.length} documents from customers to customers_anonymised`);
  }
}

async function subscribeToChanges(
  customersCollection: Collection<Customer>,
  customersAnonCollection: Collection<Customer>
): Promise<void> {
  const changeStream = customersCollection.watch();
  let documentBuffer: Customer[] = [];
  let timer: NodeJS.Timeout;

  const insertDocuments = async () => {
    const toIsert = documentBuffer;
    documentBuffer = [];

    if (toIsert.length > 0) {
      // Копирование пачки документов в коллекцию customers_anonymised
      await customersAnonCollection.insertMany(toIsert);
      console.log(
        `Inserted ${toIsert.length} documents into customers_anonymised`
      );
    }

    clearTimeout(timer);
    timer = setTimeout(insertDocuments, 1000);
  };

  changeStream.on("change", async (change) => {
    if (change.operationType === "insert") {
      // модификацировать поля документа и накопить
      const newDocument = convertDocument(change.fullDocument);
      documentBuffer.push(newDocument);

      if (documentBuffer.length >= 1000) {
        await insertDocuments();
      }
    }
  });

  // запустить таймер
  timer = setTimeout(insertDocuments, 1000);

  console.log("Listening for new documents in the customers collection...");
}

async function main() {
  try {
    // Чтение конфигурации из .env файла
    const { DB_URI } = process.env;

    if (!DB_URI) {
      throw new Error("DB_URI is not defined in the .env file");
    }

    // Подключение к MongoDB
    const client = new MongoClient(DB_URI);
    await client.connect();
    const db = client.db();

    // Получение коллекции customers и customers_anonymised
    const customersCollection = db.collection<Customer>("customers");
    const customersAnonCollection = db.collection<Customer>(
      "customers_anonymised"
    );

    const fullReindexFlagIndex = process.argv.indexOf("--full-reindex");

    if (fullReindexFlagIndex !== -1) {
      // Если задан параметр --full-reindex, выполняем полную синхронизацию
      await synchronizeDocuments(customersCollection, customersAnonCollection);
      client.close();
      process.exit(0);
    } else {
      // Иначе, подписываемся на события добавления новых документов в коллекцию customers
      await subscribeToChanges(customersCollection, customersAnonCollection);
    }
  } catch (error) {
    console.error("An error occurred:", error);
  }
}

main();
