# Notification Service

## 📋 Описание

Notification Service - это микросервис для обработки и отправки уведомлений в e-commerce системе. Сервис прослушивает события Kafka от Order Service и Payment Service, сохраняет уведомления в MongoDB и отправляет email уведомления клиентам.

## 🎯 Основные функции

- Прослушивание событий заказов из Kafka
- Прослушивание событий платежей из Kafka
- Отправка email уведомлений клиентам
- Сохранение истории уведомлений в MongoDB
- Использование HTML шаблонов для email
- Асинхронная обработка email

## ⚙️ Технический стек

- **Java 17**
- **Spring Boot 3.5.3**
- **Spring Data MongoDB**
- **Spring Kafka** (consumer)
- **Spring Mail** (для отправки email)
- **Thymeleaf** (для HTML шаблонов)
- **Spring Cloud Config Client**
- **Spring Cloud Netflix Eureka Client**
- **MongoDB**
- **Lombok**
- **Maven**

## 🗄️ Модель данных

### Notification Entity
```java
@Document
public class Notification {
    @Id
    private String id;
    
    private NotificationType type;
    private LocalDateTime notificationDate;
    private OrderConfirmation orderConfirmation;
    private PaymentConfirmation paymentConfirmation;
}
```

### NotificationType Enum
```java
public enum NotificationType {
    ORDER_CONFIRMATION,
    PAYMENT_CONFIRMATION
}
```

## 🚀 Запуск сервиса

### Предварительные условия
- Config Server (http://localhost:8888)
- Discovery Service (http://localhost:8761)
- MongoDB (localhost:27017)
- Kafka (для получения событий)
- MailDev (localhost:1025) - для тестирования email
- Java 17+
- Maven 3.6+

### Локальный запуск

```bash
# Клонирование репозитория
git clone <repository-url>
cd services/notification

# Сборка проекта
./mvnw clean install

# Запуск сервиса
./mvnw spring-boot:run
```

## 🔧 Конфигурация

### application.yml
```yaml
spring:
  application:
    name: notification-service
  config:
    import: optional:configserver:http://localhost:8888
```

### notification-service.yml (в Config Server)
```yaml
spring:
  data:
    mongodb:
      uri: mongodb://qwe:qwe@localhost:27017/notification?authSource=admin
  mail:
    host: localhost
    port: 1025
    username: 
    password: 
    properties:
      mail:
        smtp:
          trust: "*"
          auth: false
          starttls:
            enable: false

server:
  port: 8040
```

## 🏗️ Архитектура

### Структура пакетов
```
kg.manurov.ecommerce/
├── enums/
│   ├── EmailTemplates.java
│   └── NotificationType.java
├── kafka/
│   ├── NotificationsConsumer.java
│   ├── order/
│   │   ├── Customer.java
│   │   ├── OrderConfirmation.java
│   │   └── Product.java
│   └── payment/
│       ├── PaymentConfirmation.java
│       └── PaymentMethod.java
├── models/
│   └── Notification.java
├── repositories/
│   └── NotificationRepository.java
├── services/
│   └── EmailService.java
└── NotificationApplication.java
```

### Основные компоненты

#### NotificationsConsumer
Kafka consumer для обработки событий от других сервисов.

#### EmailService
Сервис для отправки email уведомлений с использованием HTML шаблонов.

#### NotificationRepository
MongoDB репозиторий для сохранения истории уведомлений.

## 📨 Kafka Consumers

### Order Confirmation Consumer
```java
@KafkaListener(topics = "order-topic")
public void consumeOrderConfirmationNotifications(OrderConfirmation orderConfirmation) throws MessagingException {
    // 1. Сохранение уведомления
    repository.save(
            Notification.builder()
                    .type(ORDER_CONFIRMATION)
                    .notificationDate(LocalDateTime.now())
                    .orderConfirmation(orderConfirmation)
                    .build()
    );
    
    // 2. Отправка email
    String customerName = orderConfirmation.customer().firstname() + " " + orderConfirmation.customer().lastname();
    emailService.sendOrderConfirmationEmail(
            orderConfirmation.customer().email(),
            customerName,
            orderConfirmation.totalAmount(),
            orderConfirmation.orderReference(),
            orderConfirmation.products()
    );
}
```

### Payment Confirmation Consumer
```java
@KafkaListener(topics = "payment-topic")
public void consumePaymentSuccessNotifications(PaymentConfirmation paymentConfirmation) throws MessagingException {
    // 1. Сохранение уведомления
    repository.save(
            Notification.builder()
                    .type(PAYMENT_CONFIRMATION)
                    .notificationDate(LocalDateTime.now())
                    .paymentConfirmation(paymentConfirmation)
                    .build()
    );
    
    // 2. Отправка email
    String customerName = paymentConfirmation.customerFirstname() + " " + paymentConfirmation.customerLastname();
    emailService.sendPaymentSuccessEmail(
            paymentConfirmation.customerEmail(),
            customerName,
            paymentConfirmation.amount(),
            paymentConfirmation.orderReference()
    );
}
```

## 📧 Email Service

### Отправка уведомления о заказе
```java
@Async
public void sendOrderConfirmationEmail(
        String destinationEmail,
        String customerName,
        BigDecimal amount,
        String orderReference,
        List<Product> products
) throws MessagingException {
    
    MimeMessage mimeMessage = mailSender.createMimeMessage();
    MimeMessageHelper messageHelper = new MimeMessageHelper(mimeMessage, MimeMessageHelper.MULTIPART_MODE_MIXED_RELATED, UTF_8.name());
    
    messageHelper.setFrom("basit.03.kg@gmail.com");
    messageHelper.setTo(destinationEmail);
    messageHelper.setSubject(ORDER_CONFIRMATION.getSubject());
    
    // Подготовка переменных для шаблона
    Map<String, Object> variables = new HashMap<>();
    variables.put("customerName", customerName);
    variables.put("totalAmount", amount);
    variables.put("orderReference", orderReference);
    variables.put("products", products);
    
    Context context = new Context();
    context.setVariables(variables);
    
    // Генерация HTML из шаблона
    String htmlTemplate = templateEngine.process(ORDER_CONFIRMATION.getTemplate(), context);
    messageHelper.setText(htmlTemplate, true);
    
    mailSender.send(mimeMessage);
}
```

### Отправка уведомления о платеже
```java
@Async
public void sendPaymentSuccessEmail(
        String destinationEmail,
        String customerName,
        BigDecimal amount,
        String orderReference
) throws MessagingException {
    
    // Аналогичная логика для шаблона payment-confirmation.html
}
```

## 📄 HTML Templates

### order-confirmation.html
```html
<!DOCTYPE html>
<html lang="en" xmlns:th="http://www.thymeleaf.org">
<head>
    <meta charset="UTF-8">
    <title>Order Details</title>
    <style>
        body { font-family: Arial, sans-serif; background-color: #f4f4f4; }
        .container { max-width: 800px; margin: 0 auto; padding: 20px; background-color: #fff; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 12px; border: 1px solid #ddd; text-align: left; }
        th { background-color: #007BFF; color: #fff; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Order Details</h1>
        <p>Customer: <span th:text="${customerName}"></span></p>
        <p>Order ID: <span th:text="${orderReference}"></span></p>
        
        <table>
            <thead>
                <tr>
                    <th>Product Name</th>
                    <th>Quantity</th>
                    <th>Price</th>
                </tr>
            </thead>
            <tbody>
                <tr th:each="product : ${products}">
                    <td th:text="${product.name}"></td>
                    <td th:text="${product.quantity}"></td>
                    <td th:text="${product.price}"></td>
                </tr>
            </tbody>
        </table>
        
        <div class="footer">
            <p>Total Amount: $<span th:text="${totalAmount}"></span></p>
        </div>
    </div>
</body>
</html>
```

### payment-confirmation.html
```html
<!DOCTYPE html>
<html lang="en" xmlns:th="http://www.thymeleaf.org">
<head>
    <meta charset="UTF-8">
    <title>Payment Confirmation</title>
    <style>
        body { font-family: Arial, sans-serif; background-color: #f4f4f4; }
        .container { max-width: 600px; margin: 0 auto; padding: 20px; background-color: #fff; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Payment Confirmation</h1>
        <p>Dear <span th:text="${customerName}"></span>,</p>
        <p>Your payment of $<span th:text="${amount}"></span> has been successfully processed.</p>
        <p>Order reference: <span th:text="${orderReference}"></span></p>
        <p>Thank you for choosing our service.</p>
    </div>
</body>
</html>
```

## 🎨 Email Templates Enum

```java
@Getter
public enum EmailTemplates {
    PAYMENT_CONFIRMATION("payment-confirmation.html", "Payment successfully processed"),
    ORDER_CONFIRMATION("order-confirmation.html", "Order confirmation");

    private final String template;
    private final String subject;

    EmailTemplates(String template, String subject) {
        this.template = template;
        this.subject = subject;
    }
}
```

## 📊 Мониторинг

### Health Check
```http
GET /actuator/health
```

### MongoDB Health Check
```json
{
  "status": "UP",
  "components": {
    "mongo": {
      "status": "UP",
      "details": {
        "version": "5.0.0"
      }
    }
  }
}
```

### Mail Health Check
```json
{
  "status": "UP",
  "components": {
    "mail": {
      "status": "UP",
      "details": {
        "location": "localhost:1025"
      }
    }
  }
}
```

## 🔗 Интеграция с другими сервисами

### События от Order Service
Order Service отправляет `OrderConfirmation` события в топик `order-topic`:

```java
// OrderConfirmation event structure
public record OrderConfirmation(
    String orderReference,
    BigDecimal totalAmount,
    PaymentMethod paymentMethod,
    Customer customer,
    List<Product> products
) {}
```

### События от Payment Service
Payment Service отправляет `PaymentConfirmation` события в топик `payment-topic`:

```java
// PaymentConfirmation event structure
public record PaymentConfirmation(
    String orderReference,
    BigDecimal amount,
    PaymentMethod paymentMethod,
    String customerFirstname,
    String customerLastname,
    String customerEmail
) {}
```

## 🐛 Устранение неполадок

### Частые проблемы

1. **MongoDB недоступен**
    - Убедитесь, что MongoDB запущен на порту 27017
    - Проверьте учетные данные (qwe/qwe)
    - Проверьте доступность базы данных notification

2. **Kafka события не обрабатываются**
    - Убедитесь, что Kafka запущен
    - Проверьте существование топиков 'order-topic' и 'payment-topic'
    - Проверьте логи Kafka consumer

3. **Email не отправляются**
    - Убедитесь, что MailDev запущен на порту 1025
    - Проверьте конфигурацию spring.mail в Config Server
    - Проверьте логи EmailService

4. **Ошибки HTML шаблонов**
    - Проверьте существование файлов в resources/templates/
    - Убедитесь в корректности синтаксиса Thymeleaf
    - Проверьте передачу переменных в Context

## 📁 Структура проекта

```
notification/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── kg/manurov/ecommerce/
│   │   │       ├── enums/
│   │   │       │   ├── EmailTemplates.java
│   │   │       │   └── NotificationType.java
```
notification/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── kg/manurov/ecommerce/
│   │   │       ├── enums/
│   │   │       │   ├── EmailTemplates.java
│   │   │       │   └── NotificationType.java
│   │   │       ├── kafka/
│   │   │       │   ├── NotificationsConsumer.java
│   │   │       │   ├── order/
│   │   │       │   │   ├── Customer.java
│   │   │       │   │   ├── OrderConfirmation.java
│   │   │       │   │   └── Product.java
│   │   │       │   └── payment/
│   │   │       │       ├── PaymentConfirmation.java
│   │   │       │       └── PaymentMethod.java
│   │   │       ├── models/
│   │   │       │   └── Notification.java
│   │   │       ├── repositories/
│   │   │       │   └── NotificationRepository.java
│   │   │       ├── services/
│   │   │       │   └── EmailService.java
│   │   │       └── NotificationApplication.java
│   │   └── resources/
│   │       ├── templates/
│   │       │   ├── order-confirmation.html
│   │       │   └── payment-confirmation.html
│   │       └── application.yml
│   └── test/
├── target/
├── pom.xml
└── README.md
```